using Azure.Storage.Blobs;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CosmosDBImport
{
    public class DocumentProcessor
    {
        private readonly BlobContainerClient _containerClient;
        private readonly DocumentCreator _documentCreator;
        private readonly ITargetBlock<RootDocument> _outputBuffer;
        private readonly CancellationToken _cancellationToken;
        private readonly int _maxParallelism;

        public DocumentProcessor(BlobContainerClient containerClient, 
            ITargetBlock<RootDocument> outputBuffer, 
            CancellationToken cancellationToken)
        {
            _containerClient = containerClient;
            _documentCreator = new DocumentCreator();
            _outputBuffer = outputBuffer;
            _cancellationToken = cancellationToken;
            _maxParallelism = AppConfig.MaxDegreeOfParallelism;
        }

        public async Task<Dictionary<string, (int originalRecords, int splitFiles, long processingTime)>> ProcessFilesAsync()
        {
            var fileDetails = new ConcurrentDictionary<string, (int, int, long)>();
            var processStopwatch = Stopwatch.StartNew();

            try
            {
                var processingBlock = new TransformBlock<string, (string name, List<RootDocument> docs)>(
                    async blobName =>
                    {
                        var result = await _documentCreator.ProcessSingleBlobAsync(_containerClient, blobName);
                        if (result.details.splitFiles > 0)
                        {
                            fileDetails.TryAdd(blobName, result.details);
                        }
                        return (blobName, result.documents);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        MaxDegreeOfParallelism = _maxParallelism,
                        CancellationToken = _cancellationToken,
                        BoundedCapacity = _maxParallelism * 2
                    }
                );

                var outputBlock = new ActionBlock<(string name, List<RootDocument> docs)>(
                    async result =>
                    {
                        try
                        {
                            foreach (var doc in result.docs)
                            {
                                if (!await _outputBuffer.SendAsync(doc, _cancellationToken))
                                {
                                    Console.WriteLine($"Failed to send document from {result.name} to output buffer");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing documents from {result.name}: {ex.Message}");
                        }
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        MaxDegreeOfParallelism = _maxParallelism,
                        CancellationToken = _cancellationToken,
                        BoundedCapacity = _maxParallelism * 2
                    }
                );

                // Link the blocks with error handling
                processingBlock.LinkTo(outputBlock, 
                    new DataflowLinkOptions 
                    { 
                        PropagateCompletion = true,
                        MaxMessages = AppConfig.BatchSize * 2
                    });

                // Process all blobs
                try
                {
                    await foreach (var blobItem in _containerClient.GetBlobsAsync(cancellationToken: _cancellationToken))
                    {
                        if (!await processingBlock.SendAsync(blobItem.Name, _cancellationToken))
                        {
                            Console.WriteLine($"Failed to send blob {blobItem.Name} to processing block");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error listing blobs: {ex.Message}");
                }

                // Mark processing as complete and wait for completion
                processingBlock.Complete();
                await Task.WhenAll(processingBlock.Completion, outputBlock.Completion);
                
                // Complete the output buffer
                _outputBuffer.Complete();
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Document processing cancelled");
                _outputBuffer.Complete();
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in document processing: {ex.Message}");
                _outputBuffer.Complete();
                throw;
            }
            finally
            {
                processStopwatch.Stop();
                Console.WriteLine($"Document processing completed in {processStopwatch.ElapsedMilliseconds}ms");
            }

            return new Dictionary<string, (int, int, long)>(fileDetails);
        }
    }
}
