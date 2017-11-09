using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDbBenchmark
{
    interface ICosmosDbApi : IDisposable
    {
        Task<int> Initialize();
        Task Insert(int taskId, string sampleJson, long numberOfDocumentsToInsert);
    }
}
