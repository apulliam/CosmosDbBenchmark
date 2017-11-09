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
        private DocumentClient Client = null;
        private Uri CollectionUri = null;
        private string PartitionKeyProperty = null;

        public DocumentDbApi()
        {
        }

        public async Task<int> Initialize()
        {
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
                  Config.AuthKey,
                  ConnectionPolicy);
            CollectionUri = UriFactory.CreateDocumentCollectionUri(Config.DatabaseName, Config.DataCollectionName);



            DocumentCollection dataCollection = GetCollectionIfExists(Client, Config.DatabaseName, Config.DataCollectionName);
            int currentCollectionThroughput = 0;

            if (Config.ShouldCleanupOnStart || dataCollection == null)
            {
                Database database = GetDatabaseIfExists(Client, Config.DatabaseName);
                if (database != null)
                {
                    var test = dataCollection.DocumentsLink;
                    await Client.DeleteDatabaseAsync(database.SelfLink);
                }

                Console.WriteLine("Creating database {0}", Config.DatabaseName);
                database = await Client.CreateDatabaseAsync(new Database { Id = Config.DatabaseName });

                Console.WriteLine("Creating collection {0} with {1} RU/s", Config.DataCollectionName, Config.CollectionThroughput);
                dataCollection = await CreatePartitionedCollectionAsync(Client, Config.DatabaseName, Config.DataCollectionName, Config.CollectionThroughput, Config.CollectionPartitionKey);

                currentCollectionThroughput = Config.CollectionThroughput;
            }
            else
            {
                OfferV2 offer = (OfferV2)Client.CreateOfferQuery().Where(o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                currentCollectionThroughput = offer.Content.OfferThroughput;

                Console.WriteLine("Found collection {0} with {1} RU/s", Config.DataCollectionName, currentCollectionThroughput);
            }
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
            //Metrics.RequestUnitsConsumed[taskId] = 0;
           
            Dictionary<string, object> newDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(sampleJson);

            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                newDictionary["id"] = Guid.NewGuid().ToString();
                newDictionary[PartitionKeyProperty] = Guid.NewGuid().ToString();

                try
                {
                    ResourceResponse<Document> response = await Client.CreateDocumentAsync(
                            CollectionUri,
                            newDictionary,
                            new RequestOptions());

                    string partition = response.SessionToken.Split(':')[0];
                    //Metrics.RequestUnitsConsumed[taskId] += response.RequestCharge;
                    //Interlocked.Increment(ref Metrics.DocumentsInserted);
                }
                catch (Exception e)
                {
                    if (e is DocumentClientException)
                    {
                        DocumentClientException de = (DocumentClientException)e;
                        // apulliam: the code below is from original perf code.  I don't understand the logic of incrementing documents inserted on HTTP Forbidden.
                        if (de.StatusCode != HttpStatusCode.Forbidden)
                        {
                            Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(newDictionary), e);
                        }
                        else
                        {
                            //Interlocked.Increment(ref Metrics.DocumentsInserted);
                        }
                    }
                }
            }

            //Interlocked.Decrement(ref Metrics.PendingTaskCount);
        }
        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        public static async Task<DocumentCollection> CreatePartitionedCollectionAsync(DocumentClient client, string databaseName, string collectionName, int collectionThroughput, string collectionPartitionKey)
        {
            DocumentCollection existingCollection = GetCollectionIfExists(client, databaseName, collectionName);

            DocumentCollection collection = new DocumentCollection();
            collection.Id = collectionName;
            collection.PartitionKey.Paths.Add(collectionPartitionKey);

            // Show user cost of running this test
            //double estimatedCostPerMonth = 0.06 * Config.CollectionThroughput;
            //double estimatedCostPerHour = estimatedCostPerMonth / (24 * 30);
            //Console.WriteLine("The collection will cost an estimated ${0} per hour (${1} per month)", Math.Round(estimatedCostPerHour, 2), Math.Round(estimatedCostPerMonth, 2));
            //Console.WriteLine("Press enter to continue ...");
            //Console.ReadLine();

            return await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = Config.CollectionThroughput });
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
