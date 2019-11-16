using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace WorkloadExecutorMongoDB
{
    public class Function
    {
        
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            foreach (var record in sqsEvent.Records)
            {
                var workload = JsonConvert.DeserializeObject<Workload>(record.Body);

                context.Logger.LogLine($"Workload Read: {workload.Read} Update: {workload.Update}");

                int count = workload.Ids.Count;
                int readworkloadcount = (int) (count * workload.Read);
                int updateworkloadcount = (int) (count * workload.Update);

                var readWorkload = workload.Ids.Take(readworkloadcount).ToList();
                var updateWorkload = workload.Ids.Take(updateworkloadcount).ToList();

                var readWorkloadExecutionTime = await ExecuteReadWorkload(readWorkload, context).ConfigureAwait(false);

                var udpateWorkloadExecutionTime =
                    await ExecuteUpdateWorkload(updateWorkload, context).ConfigureAwait(false);

                context.Logger.LogLine(
                    $"[Summary] Total {workload.Ids.Count} read&update workload, Read takes {readWorkloadExecutionTime} ms, Update takes {udpateWorkloadExecutionTime} ms");
            }

        }

        private async Task<long> ExecuteReadWorkload(List<string> ids, ILambdaContext context)
        {
            long totalReadTime = 0;

            var client = new MongoClient(
                "mongodb://34.246.18.10:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            foreach (var id in ids)
            {
                //Read
                var swRead = Stopwatch.StartNew();
                var filter = new BsonDocument("id_str", id);
                var result = await collection.FindAsync(new BsonDocumentFilterDefinition<BsonDocument>(filter)).ConfigureAwait(false);
                swRead.Stop();

                totalReadTime += swRead.ElapsedMilliseconds;
                context.Logger.LogLine($"[Read] id {id} takes {swRead.ElapsedMilliseconds} ms");
            }

            return totalReadTime;
        }

        private async Task<long> ExecuteUpdateWorkload(List<string> ids, ILambdaContext context)
        {
            long totalUpdateTime = 0;

            var client = new MongoClient(
                "mongodb://34.246.18.10:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            foreach (var id in ids)
            {
                //Update
                var swUpdate = Stopwatch.StartNew();
                var filter = new BsonDocument("id_str", id);
                var update = new BsonDocument("$set", new BsonDocument("text", $"modified by xiaoli at {DateTime.Now}"));
                await collection.UpdateOneAsync(filter, update).ConfigureAwait(false);
                swUpdate.Stop();

                totalUpdateTime += swUpdate.ElapsedMilliseconds;
                context.Logger.LogLine($"[Update] id {id} takes {swUpdate.ElapsedMilliseconds} ms");
            }

            return totalUpdateTime;
        }
    }
}
