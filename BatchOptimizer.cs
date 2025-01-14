using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDBImport
{
    public class BatchOptimizer
    {
        private const int MaxBatchSize = 100;
        private const int MaxBatchBytes = 2000000; // 2MB Cosmos DB limit
        private const int MinBatchSize = 25;

        public async Task<List<List<RootDocument>>> OptimizeBatches(List<RootDocument> documents)
        {
            var batches = new List<List<RootDocument>>();
            var currentBatch = new List<RootDocument>();
            int currentBatchSize = 0;

            foreach (var doc in documents)
            {
                var docSize = GetDocumentSize(doc);

                if (currentBatch.Count >= MaxBatchSize || currentBatchSize + docSize > MaxBatchBytes)
                {
                    if (currentBatch.Any())
                    {
                        batches.Add(new List<RootDocument>(currentBatch));
                        currentBatch.Clear();
                        currentBatchSize = 0;
                    }
                }

                currentBatch.Add(doc);
                currentBatchSize += docSize;
            }

            if (currentBatch.Any())
            {
                batches.Add(new List<RootDocument>(currentBatch));
            }

            return await OptimizePartitionDistribution(batches);
        }

        private int GetDocumentSize(RootDocument doc)
        {
            try
            {
                return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(doc)).Length;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error calculating document size: {ex.Message}");
                return 0;
            }
        }

        private async Task<List<List<RootDocument>>> OptimizePartitionDistribution(List<List<RootDocument>> batches)
        {
            var optimizedBatches = new List<List<RootDocument>>();

            foreach (var batch in batches)
            {
                var partitionGroups = batch.GroupBy(d => d.Metadata?.Filename ?? "unknown")
                                         .ToDictionary(g => g.Key, g => g.ToList());

                var currentBatch = new List<RootDocument>();
                
                foreach (var group in partitionGroups)
                {
                    foreach (var doc in group.Value)
                    {
                        currentBatch.Add(doc);

                        if (currentBatch.Count >= MaxBatchSize)
                        {
                            optimizedBatches.Add(new List<RootDocument>(currentBatch));
                            currentBatch.Clear();
                        }
                    }
                }

                if (currentBatch.Any())
                {
                    if (currentBatch.Count < MinBatchSize && optimizedBatches.Any())
                    {
                        var lastBatch = optimizedBatches[optimizedBatches.Count - 1];
                        if (lastBatch.Count + currentBatch.Count <= MaxBatchSize)
                        {
                            lastBatch.AddRange(currentBatch);
                        }
                        else
                        {
                            optimizedBatches.Add(new List<RootDocument>(currentBatch));
                        }
                    }
                    else
                    {
                        optimizedBatches.Add(new List<RootDocument>(currentBatch));
                    }
                }
            }

            return optimizedBatches;
        }

        public async Task<(int batchCount, long totalSize)> AnalyzeBatches(List<List<RootDocument>> batches)
        {
            long totalSize = 0;
            foreach (var batch in batches)
            {
                foreach (var doc in batch)
                {
                    totalSize += GetDocumentSize(doc);
                }
            }

            return (batches.Count, totalSize);
        }
    }
}
