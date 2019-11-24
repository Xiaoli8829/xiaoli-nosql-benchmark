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

                context.Logger.LogLine($"Workload Read: {workload.Read} Update: {workload.Update} Insert: {workload.Insert}");

                int count = workload.Ids.Count;
                int readworkloadcount = (int) (count * workload.Read);
                int updateworkloadcount = (int) (count * workload.Update);
                int insertworkloadcount = (int) (count * workload.Insert);

                //Read Workload Operations
                if (readworkloadcount > 0)
                {
                    var readWorkload = workload.Ids.Take(readworkloadcount).ToList();
                    var readWorkloadExecutionTimeList = await ExecuteReadWorkload(readWorkload, context).ConfigureAwait(false);

                    var maxReadLatency = readWorkloadExecutionTimeList.Max();
                    var minReadLatency = readWorkloadExecutionTimeList.Min();
                    var averageReadLatency = readWorkloadExecutionTimeList.Average();

                    string readWorkloadReport = $"Read Workload Report \r\n" +
                                                $"[Read] Operation {readWorkload.Count} \r\n" +
                                                $"[Read] AverageLatency(ms) {averageReadLatency} \r\n" +
                                                $"[Read] MinLatency(ms) {minReadLatency} \r\n" +
                                                $"[Read] MaxLatency(ms) {maxReadLatency} \r\n" +
                                                $"[Read] 95thPercentileLatency(ms) {Percentile(readWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                                $"[Read] 99thPercentileLatency(ms)  {Percentile(readWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(readWorkloadReport);
                }



                //Update Workload Operations
                if (updateworkloadcount > 0)
                {
                    var updateWorkload = workload.Ids.Take(updateworkloadcount).ToList();
                    var udpateWorkloadExecutionTimeList =
                        await ExecuteUpdateWorkload(updateWorkload, context).ConfigureAwait(false);

                    var maxUpdateLatency = udpateWorkloadExecutionTimeList.Max();
                    var minUpdateLatency = udpateWorkloadExecutionTimeList.Min();
                    var averageUpdateLatency = udpateWorkloadExecutionTimeList.Average();



                    string udpateWorkloadReport = $"Update Worload Report \r\n" +
                                                  $"[Update] Operation {updateWorkload.Count} \r\n" +
                                                  $"[Update] AverageLatency(ms) {averageUpdateLatency} \r\n" +
                                                  $"[Update] MinLatency(ms) {minUpdateLatency} \r\n" +
                                                  $"[Update] MaxLatency(ms) {maxUpdateLatency} \r\n" +
                                                  $"[Update] 95thPercentileLatency(ms) {Percentile(udpateWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                                  $"[Update] 99thPercentileLatency(ms)  {Percentile(udpateWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(udpateWorkloadReport);
                }

                //Insert Workload Operations
                if (insertworkloadcount > 0)
                {
                    var insertWorkload = workload.Ids.Take(insertworkloadcount).ToList();
                    var insertWorkloadExecutionTimeList =
                        await ExecuteInsertWorkload(insertWorkload, context).ConfigureAwait(false);

                    var maxInsertLatency = insertWorkloadExecutionTimeList.Max();
                    var minInsertLatency = insertWorkloadExecutionTimeList.Min();
                    var averageInsertLatency = insertWorkloadExecutionTimeList.Average();



                    string insertWorkloadReport = $"Insert Worload Report \r\n" +
                                                  $"[Insert] Operation {insertWorkload.Count} \r\n" +
                                                  $"[Insert] AverageLatency(ms) {averageInsertLatency} \r\n" +
                                                  $"[Insert] MinLatency(ms) {minInsertLatency} \r\n" +
                                                  $"[Insert] MaxLatency(ms) {maxInsertLatency} \r\n" +
                                                  $"[Insert] 95thPercentileLatency(ms) {Percentile(insertWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                                  $"[Insert] 99thPercentileLatency(ms)  {Percentile(insertWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(insertWorkloadReport);
                }
            }

        }

        private async Task<List<double>> ExecuteReadWorkload(List<string> ids, ILambdaContext context)
        {
            long totalReadTime = 0;
            List<double> readTimeList = new List<double>();

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

                readTimeList.Add(swRead.Elapsed.Milliseconds);
                //context.Logger.LogLine($"[Read] id {id} takes {swRead.ElapsedMilliseconds} ms");
            }

            return readTimeList;
        }

        private async Task<List<double>> ExecuteUpdateWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> updateTimeList = new List<double>();

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

                updateTimeList.Add(swUpdate.Elapsed.Milliseconds);
                //context.Logger.LogLine($"[Update] id {id} takes {swUpdate.ElapsedMilliseconds} ms");
            }

            return updateTimeList;
        }

        private async Task<List<double>> ExecuteInsertWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> insertTimeList = new List<double>();

            var client = new MongoClient(
                "mongodb://34.246.18.10:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream-insert");

            //Query List of BsonDocument by Ids
            var bsonDocuments = await QueryTwitterStream(ids, context).ConfigureAwait(false);


            foreach (var document in bsonDocuments)
            {
                //Insert
                var swInsert = Stopwatch.StartNew();
                await collection.InsertOneAsync(document).ConfigureAwait(false);
                swInsert.Stop();

                insertTimeList.Add(swInsert.Elapsed.Milliseconds);
            }

            return insertTimeList;
        }

        private async Task<List<BsonDocument>> QueryTwitterStream(List<string> ids, ILambdaContext context)
        {
            var result = new List<BsonDocument>();

            var client = new MongoClient(
                "mongodb://34.246.18.10:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            foreach (var id in ids)
            {
                var filter = new BsonDocument("id_str", id);
                var response = await collection.FindAsync(new BsonDocumentFilterDefinition<BsonDocument>(filter)).ConfigureAwait(false);              
                result.Add(response.First());
            }

            return result;
        }


        private double Percentile(double[] sequence, double excelPercentile)
        {
            Array.Sort(sequence);
            int N = sequence.Length;
            double n = (N - 1) * excelPercentile + 1;
            // Another method: double n = (N + 1) * excelPercentile;
            if (n == 1d) return sequence[0];
            else if (n == N) return sequence[N - 1];
            else
            {
                int k = (int)n;
                double d = n - k;
                return sequence[k - 1] + d * (sequence[k] - sequence[k - 1]);
            }
        }
    }
}
