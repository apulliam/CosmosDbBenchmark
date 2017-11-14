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

        public async Task<int> Initialize(BenchmarkConfig config)
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
            CollectionUri = UriFactory.CreateDocumentCollectionUri(Config.DatabaseName, Config.CollectionName);

      

            DocumentCollection dataCollection = GetCollectionIfExists(Client, Config.DatabaseName, Config.CollectionName);
            int currentCollectionThroughput = 0;

            if (dataCollection == null)
            {
                throw new Exception("This test requires an existing empty collection.");
            }
            else
            {
                OfferV2 offer = (OfferV2)Client.CreateOfferQuery().Where(o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                currentCollectionThroughput = offer.Content.OfferThroughput;
                
            }
            
            if (Config.PartitionKey != null)
                PartitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");

            return currentCollectionThroughput;
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
