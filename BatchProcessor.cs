using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CosmosDBImport
{
    public class BatchProcessor
    {
        private readonly Container _container;
        private readonly ISourceBlock<RootDocument> _inputBuffer;
        private readonly CancellationToken _cancellationToken;
        private readonly ConcurrentBag<(int count, double ru)> _ruConsumption;
        private readonly SemaphoreSlim _throttlingSemaphore;
        private readonly int _batchSize;
        private readonly int _maxConcurrentBatches;
        
        private DateTime _lastBatchTime = DateTime.UtcNow;
        private readonly TimeSpan _minTimeBetweenBatches;

        public BatchProcessor(Container container, 
            ISourceBlock<RootDocument> inputBuffer,
            CancellationToken cancellationToken)
        {
            _container = container;
            _inputBuffer = inputBuffer;
            _cancellationToken = cancellationToken;
            _ruConsumption = new ConcurrentBag<(int count, double ru)>();
            _throttlingSemaphore = new SemaphoreSlim(1);
            _batchSize = AppConfig.BatchSize;
            _maxConcurrentBatches = AppConfig.ConcurrentBatches;
            _minTimeBetweenBatches = TimeSpan.FromMilliseconds(AppConfig.MinTimeBetweenBatchesMs);
        }

        public async Task<(int totalDocs, double totalRU)> ProcessBatchesAsync()
        {
            var batchBuffer = new BatchBlock<RootDocument>(_batchSize);
            var processStopwatch = Stopwatch.StartNew();
            var batchTasks = new List<Task>();
            var semaphore = new SemaphoreSlim(_maxConcurrentBatches);

            // Link input to batch buffer
            _inputBuffer.LinkTo(batchBuffer, new DataflowLinkOptions { PropagateCompletion = true });

            // Create action block for batch processing
            var batchProcessor = new ActionBlock<RootDocument[]>(
                async batch =>
                {
                    await semaphore.WaitAsync(_cancellationToken);
                    try
                    {
                        await ProcessBatchAsync(batch.ToList());
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = _maxConcurrentBatches,
                    CancellationToken = _cancellationToken
                }
            );

            // Link batch buffer to processor
            batchBuffer.LinkTo(batchProcessor, new DataflowLinkOptions { PropagateCompletion = true });

            // Wait for completion
            await batchProcessor.Completion;

            processStopwatch.Stop();
            var totalRU = _ruConsumption.Sum(x => x.ru);
            var totalDocs = _ruConsumption.Sum(x => x.count);

            Console.WriteLine($"Batch processing completed in {processStopwatch.ElapsedMilliseconds}ms");
            return (totalDocs, totalRU);
        }

        private async Task ProcessBatchAsync(List<RootDocument> batch)
        {
            try
            {
                var batchStopwatch = Stopwatch.StartNew();
                var success = false;
                var attempts = 0;

                while (!success && attempts < AppConfig.MaxAttempts)
                {
                    try
                    {
                        attempts++;
                        await _throttlingSemaphore.WaitAsync(_cancellationToken);
                        
                        try
                        {
                            var timeSinceLastBatch = DateTime.UtcNow - _lastBatchTime;
                            if (timeSinceLastBatch < _minTimeBetweenBatches)
                            {
                                await Task.Delay(_minTimeBetweenBatches - timeSinceLastBatch, _cancellationToken);
                            }

                            var currentBatch = attempts > 3 ? batch.Take(AppConfig.SmallerBatchSize).ToList() : batch;
                            var tasks = new List<Task<ItemResponse<RootDocument>>>();

                            foreach (var doc in currentBatch)
                            {
                                var minimalDoc = DocumentCreator.MinimizeDocument(doc);
                                tasks.Add(_container.CreateItemAsync(
                                    minimalDoc,
                                    new PartitionKey(doc.Metadata.Filename),
                                    new ItemRequestOptions { EnableContentResponseOnWrite = false },
                                    _cancellationToken
                                ));
                            }

                            var results = await Task.WhenAll(tasks);
                            double totalRequestCharge = results.Sum(r => r.RequestCharge);
                            _ruConsumption.Add((currentBatch.Count, totalRequestCharge));
                            success = true;
                            _lastBatchTime = DateTime.UtcNow;

                            Console.WriteLine($"Batch processed: {currentBatch.Count} documents, {totalRequestCharge:N2} RUs, {batchStopwatch.ElapsedMilliseconds}ms");
                        }
                        finally
                        {
                            _throttlingSemaphore.Release();
                        }
                    }
                    catch (CosmosException ce) when (ce.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                    {
                        var retryAfter = ce.RetryAfter ?? TimeSpan.FromSeconds(Math.Pow(2, attempts));
                        await Task.Delay(retryAfter, _cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error in batch (attempt {attempts}/{AppConfig.MaxAttempts}): {ex.Message}");
                        await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempts)), _cancellationToken);
                    }
                }

                batchStopwatch.Stop();
                if (!success)
                {
                    Console.WriteLine($"Failed to process batch after {AppConfig.MaxAttempts} attempts");
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Batch processing cancelled");
                throw;
            }
        }
    }
}
