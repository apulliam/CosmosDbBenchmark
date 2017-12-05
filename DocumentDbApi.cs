using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosDbBenchmark
{
    internal class DocumentDbApi : ICosmosDbApi
    {
        private BenchmarkConfig Config = null;
        private DocumentClient Client = null;
        private Uri CollectionUri = null;
        private string PartitionKeyProperty = null;

        public DocumentDbApi()
        {
        }

        public async Task Initialize(BenchmarkConfig config)
        {
            Config = config;
            var documentDbEndpoint = $"https://{Config.CosmosDbName}.documents.azure.com:443/";

            var ConnectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                RequestTimeout = new TimeSpan(1, 0, 0),
                MaxConnectionLimit = 1000,
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 10,
                    MaxRetryWaitTimeInSeconds = 60
                }
            };

            
            Client = new DocumentClient(
                  new Uri(documentDbEndpoint),
                  Config.AuthorizationKey,
                  ConnectionPolicy);
            
         
            DocumentCollection dataCollection = GetCollectionIfExists(Client, Config.DatabaseName, Config.CollectionName);

            if (Config.ShouldCleanupOnStart || dataCollection == null)
            {
                Database database = GetDatabaseIfExists(Client, Config.DatabaseName);
                if (database != null)
                {
                    await Client.DeleteDatabaseAsync(database.SelfLink);
                }

                Console.WriteLine("Creating database {0}", Config.DatabaseName);
                database = await Client.CreateDatabaseAsync(new Database { Id = Config.DatabaseName });

            
                DocumentCollection collection = new DocumentCollection() { Id = Config.CollectionName };

                if (!string.IsNullOrEmpty(Config.PartitionKey))
                    collection.PartitionKey.Paths.Add(Config.PartitionKey);

                dataCollection = await Client.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(Config.DatabaseName),
                        collection,
                        new RequestOptions { OfferThroughput = Config.CollectionThroughput });


            }
            else
            {
                await VerifyCollectionThroughput(Client, dataCollection, Config.CollectionThroughput);
            }

            CollectionUri = UriFactory.CreateDocumentCollectionUri(Config.DatabaseName, Config.CollectionName);

            if (Config.PartitionKey != null)
                PartitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");
            
        }

        private static async Task VerifyCollectionThroughput(DocumentClient client, DocumentCollection dataCollection, int collectionThroughput)
        {
            OfferV2 offer = (OfferV2)client.CreateOfferQuery().Where(o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
            if (collectionThroughput != offer.Content.OfferThroughput)
            {
                await client.ReplaceOfferAsync(new OfferV2(offer, collectionThroughput));
            }
        }

        public static async Task VerifyCollectionThroughput(BenchmarkConfig config)
        {
            var documentDbEndpoint = $"https://{config.CosmosDbName}.documents.azure.com:443/";

            var ConnectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                RequestTimeout = new TimeSpan(1, 0, 0),
                MaxConnectionLimit = 1000,
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 10,
                    MaxRetryWaitTimeInSeconds = 60
                }
            };


            using (var client = new DocumentClient(
                  new Uri(documentDbEndpoint),
                  config.AuthorizationKey,
                  ConnectionPolicy))
            {
                
                var dataCollection = client.CreateDocumentCollectionQuery(
                    UriFactory.CreateDatabaseUri(config.DatabaseName))
                    .Where(c => c.Id == config.CollectionName).AsEnumerable().FirstOrDefault();

                await VerifyCollectionThroughput(client, dataCollection, config.CollectionThroughput);
            }
        }

        private async Task Cleanup()
        {
            if (Config.ShouldCleanupOnFinish)
            {
                Console.WriteLine("Deleting Database {0}", Config.DatabaseName);
                await Client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(Config.DatabaseName));
            }
        }

        public async Task Insert(int taskId,  string sampleJson, long numberOfDocumentsToInsert)
        {
            Dictionary<string, object> newDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(sampleJson);

            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                newDictionary["id"] = Guid.NewGuid().ToString();
                if (Config.PartitionKey != null)
                    newDictionary[PartitionKeyProperty] = Guid.NewGuid().ToString();

                ResourceResponse<Document> response = await Client.CreateDocumentAsync(
                        CollectionUri,
                        newDictionary,
                        new RequestOptions());

                string partition = response.SessionToken.Split(':')[0];
                 
            }
        }

       
        /// <summary>
        /// Get the database if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested database</returns>
        public static Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested collection</returns>
        public static DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }
    
        public async void Dispose()
        {
            // ToDo: implement proper Disposing pattern
            if (Client != null)
            {
                await Cleanup();
                Client.Dispose();
                Client = null;
            }
        }
    }
}
