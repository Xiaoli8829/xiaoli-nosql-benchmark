using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace WorkloadExecutorDynamoDB
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
                int readworkloadcount = (int)(count * workload.Read);
                int updateworkloadcount = (int)(count * workload.Update);

                var readWorkload = workload.Ids.Take(readworkloadcount).ToList();
                var updateWorkload = workload.Ids.Take(updateworkloadcount).ToList();

                var readWorkloadExecutionTimeList = await ExecuteReadWorkload(readWorkload, context).ConfigureAwait(false);

                var udpateWorkloadExecutionTimeList = await ExecuteUpdateWorkload(updateWorkload, context).ConfigureAwait(false);

                //context.Logger.LogLine($"[Summary] Total {workload.Ids.Count} read&update workload, Read takes {readWorkloadExecutionTime} ms, Update takes {udpateWorkloadExecutionTime} ms");


                var maxReadLatency = readWorkloadExecutionTimeList.Max();
                var minReadLatency = readWorkloadExecutionTimeList.Min();
                var averageReadLatency = readWorkloadExecutionTimeList.Average();

                var maxUpdateLatency = udpateWorkloadExecutionTimeList.Max();
                var minUpdateLatency = udpateWorkloadExecutionTimeList.Min();
                var averageUpdateLatency = udpateWorkloadExecutionTimeList.Average();

                string readWorkloadReport = $"Read Workload Report \r\n" +
                                            $"[Read] Operation {readWorkload.Count} \r\n" +
                                            $"[Read] AverageLatency(ms) {averageReadLatency} \r\n" +
                                            $"[Read] MinLatency(ms) {minReadLatency} \r\n" +
                                            $"[Read] MaxLatency(ms) {maxReadLatency} \r\n" +
                                            $"[Read] 95thPercentileLatency(ms) {Percentile(readWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                            $"[Read] 99thPercentileLatency(ms)  {Percentile(readWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                string udpateWorkloadReport = $"Update Worload Report \r\n" +
                                            $"[Update] Operation {updateWorkload.Count} \r\n" +
                                            $"[Update] AverageLatency(ms) {averageUpdateLatency} \r\n" +
                                            $"[Update] MinLatency(ms) {minUpdateLatency} \r\n" +
                                            $"[Update] MaxLatency(ms) {maxUpdateLatency} \r\n" +
                                            $"[Update] 95thPercentileLatency(ms) {Percentile(udpateWorkloadExecutionTimeList.ToArray(), 0.95)} \r\n" +
                                            $"[Update] 99thPercentileLatency(ms)  {Percentile(udpateWorkloadExecutionTimeList.ToArray(), 0.99)} \r\n";

                context.Logger.LogLine(readWorkloadReport);

                context.Logger.LogLine(udpateWorkloadReport);
            }            
        }

        private async Task<List<double>> ExecuteReadWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> readTimeList = new List<double>();

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);


            string tableName = "twitter-stream-data";
            IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);

            foreach (var id in ids)
            {
                //Measure Query
                var swQuery = Stopwatch.StartNew();
                var responseResult = await dynamoDbContext.LoadAsync<TwitterStreamModel>(id).ConfigureAwait(false);
                swQuery.Stop();

                readTimeList.Add(swQuery.Elapsed.Milliseconds);
                //context.Logger.LogLine($"[Query Workload] id {id} takes {queryTimeNs} ms");
            }

            return readTimeList;
        }

        private async Task<List<double>> ExecuteUpdateWorkload(List<string> ids, ILambdaContext context)
        {
            List<double> updateTimeList = new List<double>();

            AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
            ddbConfig.ServiceURL = "http://34.246.18.10:8000";

            AmazonDynamoDBClient amazonDynamoDbClient =
                new AmazonDynamoDBClient(ddbConfig);


            string tableName = "twitter-stream-data";
            IDynamoDBContext dynamoDbContext = new DynamoDBContext(amazonDynamoDbClient);

            foreach (var id in ids)
            {
                //Measure Update
                var responseResult = await dynamoDbContext.LoadAsync<TwitterStreamModel>(id).ConfigureAwait(false);
                responseResult.text = $" modified by xiaoli at {DateTime.Now}";

                var swUpdate = Stopwatch.StartNew();
                await dynamoDbContext.SaveAsync(responseResult).ConfigureAwait(false);
                swUpdate.Stop();

                updateTimeList.Add(swUpdate.Elapsed.Milliseconds);
                //context.Logger.LogLine($"[Update Workload] id {id} takes {swUpdate.ElapsedMilliseconds} ms");

            }

            return updateTimeList;
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
