﻿using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Lambdas.Tests
{
    public class WorkloadExecutorMongoDBTest
    {
        [Fact]
        public async Task Test_MongoDB_Aggregate()
        {
            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            var aggregate = collection.Aggregate()
                .Group(new BsonDocument { { "_id", new BsonDocument("location", "$user.location") }, { "UserCount", new BsonDocument("$sum", 1) } })
                .Sort(new BsonDocument { { "UserCount", -1 } });

            var results = await aggregate.ToListAsync();
        }


        [Fact]
        public async Task Test_MongoDB_ComplexQuery()
        {
            var client = new MongoClient(
                "mongodb://172.31.53.247:27017"
            );
            var database = client.GetDatabase("twitter");
            var collection = database.GetCollection<BsonDocument>("stream");

            //var filter = new BsonDocument("id_str", "1193455032219234306");

            var builder = Builders<BsonDocument>.Filter;
            var filter = builder.Eq("user.verified", true)
                         & builder.Gt("user.followers_count", 100)
                         & builder.Not(builder.Eq("user.location", BsonNull.Value));
            using (var cursor = await collection.FindAsync(filter).ConfigureAwait(false))
            {
                while (cursor.MoveNext())
                {
                    foreach (var doc in cursor.Current)
                    {
                        var userDocument = doc.Elements.FirstOrDefault(x => x.Name == "user").Value.ToBsonDocument();

                        var userName = userDocument.Elements.FirstOrDefault(x => x.Name == "name").Value.AsString;
                        var userLocation = userDocument.Elements.FirstOrDefault(x => x.Name == "location").Value ?? userDocument.Elements.FirstOrDefault(x => x.Name == "location").Value.AsString;
                        var userVerified = userDocument.Elements.FirstOrDefault(x => x.Name == "verified").Value.AsBoolean;
                        var userFriendsCount = userDocument.Elements.FirstOrDefault(x => x.Name == "friends_count").Value.AsInt32;

                        Console.WriteLine($"userName {userName} \n userLocation {userLocation} \n userVerified {userVerified} \n  userFriendsCount {userFriendsCount}");
                    }
                }
            }

            Assert.True(true);
        }
    }
}