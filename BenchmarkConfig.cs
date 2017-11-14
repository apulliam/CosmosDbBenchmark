using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDbBenchmark
{
    public class BenchmarkConfig
    {
        public string CosmosDbName;
        public string CosmosDbApi;
        public string AuthorizationKey;
        public string DatabaseName;
        public string CollectionName;
        public int CollectionThroughput;
        public long NumberOfDocumentsToInsert;
        public string PartitionKey;
        public bool ShouldCleanupOnStart;
        public bool ShouldCleanupOnFinish;
        public string DocumentTemplateFile;
        public int DegreeOfParallelism;
        public bool MongoInsertMany;
        public int MongoInsertManyBatchSize;
    }
}
