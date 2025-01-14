using Azure.Storage.Blobs;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Azure.Cosmos;

namespace CosmosDBImport
{
    public class DocumentCreator
    {
        private static readonly JsonSerializerSettings jsonSettings = new JsonSerializerSettings 
        { 
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.None
        };

        public async Task<(List<RootDocument> documents, Dictionary<string, (int originalRecords, int splitFiles, long processingTime)> fileDetails)> 
            ProcessBlobFilesAsync(BlobContainerClient containerClient)
        {
            var fileProcessingDetails = new Dictionary<string, (int originalRecords, int splitFiles, long processingTime)>();
            var blobItems = new List<string>();
            
            await foreach (var blobItem in containerClient.GetBlobsAsync())
            {
                blobItems.Add(blobItem.Name);
            }

            // Process blobs in parallel with a max degree of parallelism
            var results = await Task.WhenAll(
                blobItems.Select(blobName => ProcessSingleBlobAsync(containerClient, blobName))
            );

            var documents = results.SelectMany(r => r.documents).ToList();
            
            // Fixed tuple deconstruction
            for (int i = 0; i < results.Length; i++)
            {
                fileProcessingDetails.Add(blobItems[i], results[i].details);
            }

            return (documents, fileProcessingDetails);
        }

        public async Task<(List<RootDocument> documents, (int originalRecords, int splitFiles, long processingTime) details)> 
            ProcessSingleBlobAsync(BlobContainerClient containerClient, string blobName)
        {
            var processStopwatch = System.Diagnostics.Stopwatch.StartNew();
            var documents = new List<RootDocument>();

            try
            {
                Console.WriteLine($"Processing blob: {blobName}");
                var blobClient = containerClient.GetBlobClient(blobName);
                var memoryStream = new MemoryStream();
                await blobClient.DownloadToAsync(memoryStream);

                memoryStream.Position = 0;
                string jsonContent = Encoding.UTF8.GetString(memoryStream.ToArray());

                var originalDocuments = JsonConvert.DeserializeObject<List<RootDocument>>(jsonContent);
                int totalNewDocuments = 0;

                foreach (var originalDoc in originalDocuments)
                {
                    foreach (var body in originalDoc.Body)
                    {
                        var newDoc = CreateSplitDocument(originalDoc, body);
                        documents.Add(newDoc);
                        totalNewDocuments++;
                    }
                }

                processStopwatch.Stop();
                return (documents, (originalDocuments.Count, totalNewDocuments, processStopwatch.ElapsedMilliseconds));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing blob {blobName}: {ex.Message}");
                processStopwatch.Stop();
                return (new List<RootDocument>(), (0, 0, processStopwatch.ElapsedMilliseconds));
            }
        }

        public async Task<List<RootDocument>> ProcessSingleFileAsync(string filename, BlobContainerClient containerClient)
        {
            try
            {
                var blobClient = containerClient.GetBlobClient(filename);
                if (!await blobClient.ExistsAsync())
                {
                    Console.WriteLine($"File {filename} not found");
                    return new List<RootDocument>();
                }

                var memoryStream = new MemoryStream();
                await blobClient.DownloadToAsync(memoryStream);
                memoryStream.Position = 0;
                
                var content = Encoding.UTF8.GetString(memoryStream.ToArray());
                var documents = new List<RootDocument>();

                try
                {
                    // Try to deserialize as array first
                    var docArray = JsonConvert.DeserializeObject<List<RootDocument>>(content);
                    if (docArray != null && docArray.Count > 0)
                    {
                        foreach (var doc in docArray)
                        {
                            if (doc.Body != null && doc.Body.Count > 0)
                            {
                                foreach (var body in doc.Body)
                                {
                                    var splitDoc = CreateSplitDocument(doc, body);
                                    documents.Add(splitDoc);
                                }
                            }
                        }
                    }
                    else
                    {
                        // Try to deserialize as single document
                        var singleDoc = JsonConvert.DeserializeObject<RootDocument>(content);
                        if (singleDoc != null)
                        {
                            singleDoc.Metadata = singleDoc.Metadata ?? new Metadata();
                            singleDoc.Metadata.Filename = filename;
                            documents.Add(singleDoc);
                        }
                    }
                }
                catch (JsonReaderException ex)
                {
                    Console.WriteLine($"JSON parsing error in file {filename}: {ex.Message}");
                    return new List<RootDocument>();
                }

                return documents;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing file {filename}: {ex.Message}");
                return new List<RootDocument>();
            }
        }

        private RootDocument CreateSplitDocument(RootDocument originalDoc, Body body)
        {
            return new RootDocument
            {
                Id = $"{originalDoc.Metadata?.Filename ?? "unknown"}-{Guid.NewGuid()}",
                Metadata = originalDoc.Metadata == null ? null : new Metadata
                {
                    Filename = originalDoc.Metadata.Filename,
                    Class = originalDoc.Metadata.Class
                },
                Header = originalDoc.Header,
                Body = new List<Body> { body ?? new Body() },
                Footer = originalDoc.Footer
            };
        }

        public static RootDocument MinimizeDocument(RootDocument doc)
        {
            try
            {
                return JsonConvert.DeserializeObject<RootDocument>(
                    JsonConvert.SerializeObject(doc, jsonSettings)
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error minimizing document: {ex.Message}");
                return doc; // Return original document if minimization fails
            }
        }

        // Add new method for bulk insertion
        public async Task BulkInsertToCosmosDBAsync(
            CosmosClient cosmosClient,
            string databaseId,
            string containerId,
            IEnumerable<RootDocument> documents,
            int batchSize = 100)
        {
            var container = cosmosClient.GetContainer(databaseId, containerId);
            var tasks = new List<Task>();
            var batch = new List<RootDocument>();

            foreach (var doc in documents)
            {
                batch.Add(doc);
                
                if (batch.Count >= batchSize)
                {
                    var batchToProcess = batch.ToList();
                    batch.Clear();
                    
                    var task = ProcessBatchAsync(container, batchToProcess);
                    tasks.Add(task);

                    // Limit concurrent tasks
                    if (tasks.Count >= 10)
                    {
                        await Task.WhenAny(tasks);
                        tasks.RemoveAll(t => t.IsCompleted);
                    }
                }
            }

            // Process remaining documents
            if (batch.Any())
            {
                tasks.Add(ProcessBatchAsync(container, batch));
            }

            await Task.WhenAll(tasks);
        }

        private async Task ProcessBatchAsync(Container container, List<RootDocument> batch)
        {
            var tasks = batch.Select(doc => {
                return container.UpsertItemAsync(
                    MinimizeDocument(doc),
                    new PartitionKey(doc.Metadata?.Filename ?? "unknown"),
                    new ItemRequestOptions { EnableContentResponseOnWrite = false }
                );
            });

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in batch processing: {ex.Message}");
                // Could implement retry logic here
            }
        }
    }
}