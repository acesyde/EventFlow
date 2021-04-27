// The MIT License (MIT)
// 
// Copyright (c) 2015-2020 Rasmus Mikkelsen
// Copyright (c) 2015-2020 eBay Software Foundation
// https://github.com/eventflow/EventFlow
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

using EventFlow.Core;
using EventFlow.Extensions;
using EventFlow.Logs;
using EventFlow.Snapshots;
using EventFlow.Snapshots.Stores;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using EventFlow.DynamoDb.ValueObjects;

namespace EventFlow.DynamoDb.SnapshotStores
{
    public class DynamoDbSnapshotPersistence : ISnapshotPersistence
    {
        private static string SnapShotsCollectionName = "snapShots";
        private readonly ILog _log;
        private readonly IAmazonDynamoDB _amazonDynamoDb;
        private readonly IDynamoDBContext _dynamoDbContext;

        public DynamoDbSnapshotPersistence(
            ILog log,
            IAmazonDynamoDB amazonDynamoDb)
        {
            _log = log;
            _amazonDynamoDb = amazonDynamoDb;
            _dynamoDbContext = new DynamoDBContext(_amazonDynamoDb);
        }

        public async Task<CommittedSnapshot> GetSnapshotAsync(
            Type aggregateType,
            IIdentity identity,
            CancellationToken cancellationToken)
        {

            var queryFilter = new ScanFilter();
            queryFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateName), ScanOperator.Equal, aggregateType.GetAggregateName().Value);
            queryFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateId), ScanOperator.Equal, identity.Value);
            
            var result = await _dynamoDbContext.FromScanAsync<DynamoDbSnapshotDataModel>(new ScanOperationConfig
            {
                Filter = queryFilter,
                Limit = 1,
                ConditionalOperator = ConditionalOperatorValues.And
            }).GetNextSetAsync(cancellationToken);

            var mongoDbSnapshotDataModel = result
                .FirstOrDefault();

            if (mongoDbSnapshotDataModel == null)
            {
                return null;
            }

            return new CommittedSnapshot(
                mongoDbSnapshotDataModel.Metadata,
                mongoDbSnapshotDataModel.Data);
        }

        public async Task SetSnapshotAsync(
            Type aggregateType,
            IIdentity identity,
            SerializedSnapshot serializedSnapshot,
            CancellationToken cancellationToken)
        {
            var mongoDbSnapshotDataModel = new MongoDbSnapshotDataModel
            {
                _id = ObjectId.GenerateNewId(DateTime.UtcNow),
                AggregateId = identity.Value,
                AggregateName = aggregateType.GetAggregateName().Value,
                AggregateSequenceNumber = serializedSnapshot.Metadata.AggregateSequenceNumber,
                Metadata = serializedSnapshot.SerializedMetadata,
                Data = serializedSnapshot.SerializedData,
            };

            var collection = _amazonDynamoDb.GetCollection<MongoDbSnapshotDataModel>(SnapShotsCollectionName);
            var filterBuilder = Builders<MongoDbSnapshotDataModel>.Filter;

            var filter = filterBuilder.Eq(model => model.AggregateName, aggregateType.GetAggregateName().Value) &
                         filterBuilder.Eq(model => model.AggregateId, identity.Value) &
                         filterBuilder.Eq(model => model.AggregateSequenceNumber, serializedSnapshot.Metadata.AggregateSequenceNumber);

            await collection.DeleteManyAsync(filter, cancellationToken);
            await collection.InsertOneAsync(mongoDbSnapshotDataModel, cancellationToken: cancellationToken);
        }

        public Task DeleteSnapshotAsync(
            Type aggregateType,
            IIdentity identity,
            CancellationToken cancellationToken)
        {
            var collection = _amazonDynamoDb.GetCollection<MongoDbSnapshotDataModel>(SnapShotsCollectionName);
            var filterBuilder = Builders<MongoDbSnapshotDataModel>.Filter;

            var filter = filterBuilder.Eq(model => model.AggregateName, aggregateType.GetAggregateName().Value) &
                         filterBuilder.Eq(model => model.AggregateId, identity.Value);
            return collection.DeleteManyAsync(filter, cancellationToken);
        }

        public Task PurgeSnapshotsAsync(CancellationToken cancellationToken)
        {
            var collection = _amazonDynamoDb.GetCollection<MongoDbSnapshotDataModel>(SnapShotsCollectionName);
            var filter = new BsonDocument();
            return collection.DeleteManyAsync(filter, cancellationToken);
        }

        public Task PurgeSnapshotsAsync(
            Type aggregateType, 
            CancellationToken cancellationToken)
        {
            var collection = _amazonDynamoDb.GetCollection<MongoDbSnapshotDataModel>(SnapShotsCollectionName);
            var filter = Builders<MongoDbSnapshotDataModel>.Filter.Eq(model => model.AggregateName, aggregateType.GetAggregateName().Value);
            return collection.DeleteManyAsync(filter, cancellationToken);
        }        
    }
}
