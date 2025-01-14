using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CosmosDBImport
{
    class Program
    {
        private static readonly CancellationTokenSource _cancelSource = new CancellationTokenSource();
        private static readonly int ProcessorCount = Environment.ProcessorCount;

        static async Task Main(string[] args)
        {
            try
            {
                // Load configuration
                IConfiguration configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .Build();

                AppConfig.CosmosConnectionString = configuration["CosmosDbConnectionString"];
                AppConfig.StorageConnectionString = configuration["BlobConnectionString"];
                AppConfig.ConcurrentBatches = configuration.GetValue<int>("ConcurrentBatches");
                AppConfig.BatchSize = configuration.GetValue<int>("BatchSize");

                var totalStopwatch = Stopwatch.StartNew();
                var blobServiceClient = new BlobServiceClient(AppConfig.StorageConnectionString);
                var inputContainerClient = blobServiceClient.GetBlobContainerClient(AppConfig.InputContainerName);

                var cosmosClientOptions = new CosmosClientOptions
                {
                    AllowBulkExecution = true,
                    MaxRetryAttemptsOnRateLimitedRequests = 9,
                    MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
                    ConnectionMode = ConnectionMode.Direct,
                    SerializerOptions = new CosmosSerializationOptions
                    {
                        IgnoreNullValues = true
                    }
                };

                using var cosmosClient = new CosmosClient(AppConfig.CosmosConnectionString, cosmosClientOptions);
                Database database = (await cosmosClient.CreateDatabaseIfNotExistsAsync(AppConfig.DatabaseId)).Database;
                Container container = database.GetContainer(AppConfig.ContainerId);

                int ruCapacity = await GetContainerRUCapacity(container);
                Console.WriteLine($"Starting processing with {ruCapacity} RU/s capacity");

                // Create document buffer
                var bufferOptions = new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = AppConfig.BatchSize * 4,
                    CancellationToken = _cancelSource.Token
                };
                var documentBuffer = new BufferBlock<RootDocument>(bufferOptions);

                // Create document processor
                var documentProcessor = new DocumentProcessor(inputContainerClient, documentBuffer, _cancelSource.Token);
                var processingTask = documentProcessor.ProcessFilesAsync();
                
                // Collect documents
                var documents = new List<RootDocument>();
                while (await documentBuffer.OutputAvailableAsync(_cancelSource.Token))
                {
                    while (documentBuffer.TryReceive(out var doc))
                    {
                        documents.Add(doc);
                    }
                }

                Console.WriteLine($"Collected {documents.Count} documents for processing");

                // Process with optimized batch processor
                var batchProcessor = new OptimizedBatchProcessor(container, ruCapacity);
                var batchStopwatch = Stopwatch.StartNew();
                await batchProcessor.ProcessDocuments(documents);
                batchStopwatch.Stop();

                // Get file processing results
                var fileDetails = await processingTask;

                totalStopwatch.Stop();
                OutputResults(fileDetails, documents.Count, batchStopwatch.ElapsedMilliseconds, totalStopwatch.ElapsedMilliseconds);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\nOperation cancelled by user.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Critical error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
            finally
            {
                _cancelSource.Dispose();
            }
        }

        private static async Task<int> GetContainerRUCapacity(Container container)
        {
            try
            {
                var throughput = await container.ReadThroughputAsync();
                return throughput ?? 50000;
            }
            catch
            {
                return 50000;
            }
        }

        private static void OutputResults(
            Dictionary<string, (int originalRecords, int splitFiles, long processingTime)> fileDetails,
            int totalDocs, 
            long batchTime,
            long totalTime)
        {
            Console.WriteLine("\nProcessing Summary:");
            Console.WriteLine($"Files Processed: {fileDetails.Count}");
            Console.WriteLine($"Total Documents: {totalDocs:N0}");
            Console.WriteLine($"Batch Processing Time: {batchTime:N0}ms");
            Console.WriteLine($"Total Time: {totalTime:N0}ms");
            Console.WriteLine($"Average Time per Document: {(totalTime / Math.Max(1, totalDocs)):N2}ms");
        }
    }
}