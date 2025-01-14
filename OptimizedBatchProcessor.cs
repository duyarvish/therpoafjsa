using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace CosmosDBImport
{
    public class OptimizedBatchProcessor
    {
        private readonly Container _container;
        private readonly int _ruCapacity;
        private const int BatchSize = 100;
        private readonly SemaphoreSlim _throttler;
        private DateTime _lastBatchTime = DateTime.UtcNow;

        public OptimizedBatchProcessor(Container container, int ruCapacity)
        {
            _container = container;
            _ruCapacity = ruCapacity;
            _throttler = new SemaphoreSlim(4); // Allow more concurrent operations
        }

        public async Task ProcessDocuments(List<RootDocument> documents)
        {
            Console.WriteLine($"Processing {documents.Count} documents");
            
            var batches = documents
                .Select((doc, index) => new { doc, index })
                .GroupBy(x => x.index / BatchSize)
                .Select(g => g.Select(x => x.doc).ToList())
                .ToList();

            var tasks = new List<Task>();
            var processedBatches = 0;
            var totalBatches = batches.Count;

            foreach (var batch in batches)
            {
                await _throttler.WaitAsync();

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await ProcessBatchAsync(batch);
                        var completed = Interlocked.Increment(ref processedBatches);
                        if (completed % 10 == 0) // Reduce logging frequency
                        {
                            Console.WriteLine($"Progress: {completed}/{totalBatches} batches");
                        }
                    }
                    finally
                    {
                        _throttler.Release();
                    }
                }));

                if (tasks.Count >= 20) // Increased parallel operations
                {
                    var completedTask = await Task.WhenAny(tasks);
                    tasks.Remove(completedTask);
                }
            }

            await Task.WhenAll(tasks);
        }

        private async Task ProcessBatchAsync(List<RootDocument> batch)
        {
            var attempts = 0;
            const int maxAttempts = 5; // Reduced max attempts

            while (attempts < maxAttempts)
            {
                try
                {
                    var tasks = batch.Select(doc => _container.CreateItemAsync(
                        doc,
                        new PartitionKey(doc.Metadata.Filename),
                        new ItemRequestOptions { EnableContentResponseOnWrite = false }
                    )).ToList();

                    await Task.WhenAll(tasks);
                    return;
                }
                catch (CosmosException ce) when (ce.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    attempts++;
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
                catch (Exception)
                {
                    attempts++;
                    if (attempts >= maxAttempts) throw;
                    await Task.Delay(TimeSpan.FromMilliseconds(100 * attempts));
                }
            }
        }
    }
}