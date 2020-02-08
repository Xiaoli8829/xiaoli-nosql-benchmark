using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace TwitterStreamConsumerMongoDB
{
    public class Function
    {

        public async Task FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
        {
            context.Logger.LogLine($"Number of records from Kinesis: {kinesisEvent.Records.Count}");

            var sw = Stopwatch.StartNew();

            foreach (var record in kinesisEvent.Records)
            {              
                string recordData = GetRecordContents(record.Kinesis);
             
                await SaveObjectToMongoDb(recordData).ConfigureAwait(false);
                //await SaveImageToMongoDb(recordData).ConfigureAwait(false);
            }

            sw.Stop();
            context.Logger.LogLine($"Total run time: {sw.ElapsedMilliseconds} ms");
        }

        private string GetRecordContents(KinesisEvent.Record streamRecord)
        {
            using (var reader = new StreamReader(streamRecord.Data, Encoding.ASCII))
            {
                return reader.ReadToEnd();
            }
        }

        private async Task SaveObjectToMongoDb(string jsonPayload)
        {
            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            MongoDB.Bson.BsonDocument document
                = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(jsonPayload);

            //var sw = Stopwatch.StartNew();
            await collection.InsertOneAsync(document).ConfigureAwait(false);       
            //sw.Stop();
            //Console.WriteLine($"Insert document take {sw.ElapsedMilliseconds} ms");
        }

        private async Task SaveImageToMongoDb(string jsonPayload)
        {
            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("image-stream");

            try
            {

                JObject jobject = JObject.Parse(jsonPayload);
                var id = (long)jobject["id"];

                foreach (var jtoken in jobject["extended_entities"]["media"])
                {
                    string media_url = (string)jtoken["media_url"];

                    if (!string.IsNullOrEmpty(media_url))
                    {
                        //Download Image
                        WebClient webClient = new WebClient();

                        System.IO.Stream mediaStream = webClient.OpenRead(media_url);
                        var mediaBytes = ReadFully(mediaStream);

                        if (mediaBytes.Length > 0)
                        {
                            var idBason = new BsonElement("id", new BsonInt64(id));
                            var imageurlBason = new BsonElement("image", new BsonString(media_url));
                            var imagedataBason = new BsonElement("streamdata", new BsonBinaryData(mediaBytes));

                            MongoDB.Bson.BsonDocument newDoc = new BsonDocument(new List<BsonElement>
                            {
                                idBason,
                                imageurlBason,
                                imagedataBason
                            });

                            var sw = Stopwatch.StartNew();
                            await collection.InsertOneAsync(newDoc).ConfigureAwait(false);
                            sw.Stop();
                            Console.WriteLine($"Insert Image {media_url} for Id: {id} takes {sw.ElapsedMilliseconds} ms");
                        }
                    }
                } 
                
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message} {e.InnerException?.Message}");
            }
        }

        private static byte[] ReadFully(System.IO.Stream input)
        {
            byte[] buffer = new byte[16 * 1024];
            using (MemoryStream ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }
    }
}