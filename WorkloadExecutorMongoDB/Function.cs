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
            double totalWorkloadms = 0;
            double totalCount = 0;
            var sw = Stopwatch.StartNew();

            foreach (var record in sqsEvent.Records)
            {
                var workload = JsonConvert.DeserializeObject<Workload>(record.Body);

                int multiThreadCount = Convert.ToInt32(workload.Thread);

                context.Logger.LogLine($"========== multiThreadCount - {multiThreadCount}================");

                context.Logger.LogLine($"Workload Read: {workload.Read} Update: {workload.Update} Insert: {workload.Insert} Complex Query Times {workload.ComplexQuery}");

                int count = workload.Ids.Count;
                int readworkloadcount = (int) (count * workload.Read);
                int updateworkloadcount = (int) (count * workload.Update);
                int insertworkloadcount = (int) (count * workload.Insert);

                //Read Workload Operations
                if (readworkloadcount > 0)
                {
                    var readWorkload = workload.Ids.Take(readworkloadcount).ToList();

                    var swreadworkload = Stopwatch.StartNew();

                    //Multi-Threads
                    var multiThreadsResult = await readWorkload.ForEachSemaphoreAsync(multiThreadCount, x =>
                    {
                        return ExecuteReadWorkload(new List<string>
                        {
                            x
                        }, context);
                    }).ConfigureAwait(false);

                    swreadworkload.Stop();
                    totalWorkloadms += swreadworkload.Elapsed.TotalMilliseconds;

                    var readWorkloadExecutionTimeList = multiThreadsResult.SelectMany(x => x).ToList();

                    context.Logger.LogLine($"readWorkloadExecutionTimeList - {string.Join(" ms,", readWorkloadExecutionTimeList)}");

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

                    var swupdateworkload = Stopwatch.StartNew();

                    //Multi Threads
                    var multiThreadsResult = await updateWorkload.ForEachSemaphoreAsync(multiThreadCount, x =>
                    {
                        return ExecuteUpdateWorkload(new List<string>
                        {
                            x
                        }, context);
                    }).ConfigureAwait(false);

                    swupdateworkload.Stop();
                    totalWorkloadms += swupdateworkload.Elapsed.TotalMilliseconds;

                    var udpateWorkloadExecutionTimeList = multiThreadsResult.SelectMany(x => x).ToList();

                    context.Logger.LogLine($"udpateWorkloadExecutionTimeList - {string.Join(" ms,", udpateWorkloadExecutionTimeList)}");

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

                    var swinsertworkload = Stopwatch.StartNew();

                    //Multi Threads
                    var multiThreadsResult = await insertWorkload.ForEachSemaphoreAsync(multiThreadCount, x =>
                    {
                        return ExecuteInsertWorkload(new List<string>
                        {
                            x
                        }, context);
                    }).ConfigureAwait(false);

                    swinsertworkload.Stop();
                    totalWorkloadms += swinsertworkload.Elapsed.TotalMilliseconds;

                    var insertWorkloadExecutionTimeList = multiThreadsResult.SelectMany(x => x).ToList();

                    context.Logger.LogLine($"insertWorkloadExecutionTimeList - {string.Join(" ms,", insertWorkloadExecutionTimeList)}");

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

                //Complex Query Workload
                if (workload.ComplexQuery > 0)
                {
                    var complexQueryTimes = await ExecuteComplexQueryWorkload(workload.ComplexQuery).ConfigureAwait(false);

                    var maxReadLatency = complexQueryTimes.Max();
                    var minReadLatency = complexQueryTimes.Min();
                    var averageReadLatency = complexQueryTimes.Average();

                    string queryWorkloadReport = $"Complex Query Workload Report \r\n" +
                                                $"[Complex Query] Operation {workload.ComplexQuery} \r\n" +
                                                $"[Complex Query] AverageLatency(ms) {averageReadLatency} \r\n" +
                                                $"[Complex Query] MinLatency(ms) {minReadLatency} \r\n" +
                                                $"[Complex Query] MaxLatency(ms) {maxReadLatency} \r\n" +
                                                $"[Complex Query] 95thPercentileLatency(ms) {Percentile(complexQueryTimes.ToArray(), 0.95)} \r\n" +
                                                $"[Complex Query] 99thPercentileLatency(ms)  {Percentile(complexQueryTimes.ToArray(), 0.99)} \r\n";

                    context.Logger.LogLine(queryWorkloadReport);
                    totalWorkloadms += complexQueryTimes.Sum();
                }

                totalCount += count;
            }

            sw.Stop();
            context.Logger.LogLine($"Total function runtime - {sw.Elapsed.TotalMilliseconds} ms");
            context.Logger.LogLine($"Total workload runtime - {totalWorkloadms} ms");
            context.Logger.LogLine($"Throughput - {totalCount / totalWorkloadms * 1000} ops/s");
        }

        private async Task<List<double>> ExecuteReadWorkload(List<string> ids, ILambdaContext context)
        {
            long totalReadTime = 0;
            List<double> readTimeList = new List<double>();

            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
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

                readTimeList.Add(swRead.Elapsed.TotalMilliseconds);
            }

            return readTimeList;
        }

        private async Task<List<double>> ExecuteUpdateWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> updateTimeList = new List<double>();

            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
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

                updateTimeList.Add(swUpdate.Elapsed.TotalMilliseconds);
            }

            return updateTimeList;
        }

        private async Task<List<double>> ExecuteInsertWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> insertTimeList = new List<double>();

            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
            );
            var database = client.GetDatabase("twitter-insert");
            var collection = database.GetCollection<BsonDocument>("stream");

            //Query List of BsonDocument by Ids
            var bsonDocuments = await QueryTwitterStream(ids, context).ConfigureAwait(false);


            foreach (var document in bsonDocuments)
            {
                try
                {
                    //Insert
                    var swInsert = Stopwatch.StartNew();
                    await collection.InsertOneAsync(document).ConfigureAwait(false);
                    swInsert.Stop();

                    insertTimeList.Add(swInsert.Elapsed.TotalMilliseconds);
                }
                catch (Exception e)
                {
                    context.Logger.LogLine($"Error: {e.Message} Trace {e.StackTrace}");
                }
                
            }

            return insertTimeList;
        }

        private async Task<List<BsonDocument>> QueryTwitterStream(List<string> ids, ILambdaContext context)
        {
            var result = new List<BsonDocument>();

            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
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

        private async Task<List<double>> ExecuteComplexQueryWorkload(int queryTimes)
        {
            var complexQueryTime = new List<double>();
            //
            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            for (int i = 1; i < queryTimes; i++)
            {
                //Complex Query
                var sw = Stopwatch.StartNew();

                var builder = Builders<BsonDocument>.Filter;
                var filter = builder.Eq("user.verified", true)
                             & builder.Gt("user.followers_count", 100)
                             & builder.Not(builder.Eq("user.location", BsonNull.Value));

                var cursor = await collection.FindAsync(filter).ConfigureAwait(false);
                while (cursor.MoveNext())
                {

                }
                sw.Stop();
                complexQueryTime.Add(sw.Elapsed.TotalMilliseconds);
            }

            //
            return complexQueryTime;
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
