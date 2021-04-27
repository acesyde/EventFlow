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

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using EventFlow.Core;
using EventFlow.DynamoDb.ValueObjects;
using EventFlow.Extensions;
using EventFlow.Logs;
using EventFlow.Snapshots;
using EventFlow.Snapshots.Stores;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EventFlow.DynamoDb.SnapshotStores
{
    public class DynamoDbSnapshotPersistence : ISnapshotPersistence
    {
        private readonly ILog _log;
        private readonly IDynamoDBContext _dynamoDbContext;

        public DynamoDbSnapshotPersistence(
            ILog log,
            IAmazonDynamoDB amazonDynamoDb)
        {
            _log = log;
            _dynamoDbContext = new DynamoDBContext(amazonDynamoDb);
        }

        public async Task<CommittedSnapshot> GetSnapshotAsync(
            Type aggregateType,
            IIdentity identity,
            CancellationToken cancellationToken)
        {

            var queryFilter = new ScanFilter();
            queryFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateName), ScanOperator.Equal, aggregateType.GetAggregateName().Value);
            queryFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateId), ScanOperator.Equal, identity.Value);

            var search = _dynamoDbContext.FromScanAsync<DynamoDbSnapshotDataModel>(new ScanOperationConfig
            {
                Filter = queryFilter,
                Limit = 25,
                ConditionalOperator = ConditionalOperatorValues.And,
            });

            List<DynamoDbSnapshotDataModel> dataModels = new List<DynamoDbSnapshotDataModel>();

            do
            {
                dataModels.AddRange(await search.GetNextSetAsync(cancellationToken));
            } while (!search.IsDone);

            var snapshotDataModel = dataModels
                .OrderByDescending(m => m.AggregateSequenceNumber)
                .FirstOrDefault();

            if (snapshotDataModel == null)
            {
                return null;
            }

            return new CommittedSnapshot(
                snapshotDataModel.Metadata,
                snapshotDataModel.Data);
        }

        public async Task SetSnapshotAsync(
            Type aggregateType,
            IIdentity identity,
            SerializedSnapshot serializedSnapshot,
            CancellationToken cancellationToken)
        {
            var snapshotDataModel = new DynamoDbSnapshotDataModel
            {
                Id = Guid.NewGuid().ToString(),
                AggregateId = identity.Value,
                AggregateName = aggregateType.GetAggregateName().Value,
                AggregateSequenceNumber = serializedSnapshot.Metadata.AggregateSequenceNumber,
                Metadata = serializedSnapshot.SerializedMetadata,
                Data = serializedSnapshot.SerializedData,
            };

            var scanFilter = new ScanFilter();
            scanFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateName), ScanOperator.Equal, aggregateType.GetAggregateName().Value);
            scanFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateId), ScanOperator.Equal, identity.Value);
            scanFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateSequenceNumber), ScanOperator.Equal, serializedSnapshot.Metadata.AggregateSequenceNumber);

            var search = _dynamoDbContext.FromScanAsync<DynamoDbSnapshotDataModel>(new ScanOperationConfig
            {
                Filter = scanFilter,
                Limit = 25,
                ConditionalOperator = ConditionalOperatorValues.And,
            });

            List<DynamoDbSnapshotDataModel> dataModels = new List<DynamoDbSnapshotDataModel>();

            do
            {
                dataModels.AddRange(await search.GetNextSetAsync(cancellationToken));
            } while (!search.IsDone);

            var batchWrite = _dynamoDbContext.CreateBatchWrite<DynamoDbSnapshotDataModel>();
            batchWrite.AddDeleteItems(dataModels);
            await batchWrite.ExecuteAsync(cancellationToken);

            await _dynamoDbContext.SaveAsync(snapshotDataModel, cancellationToken: cancellationToken);
        }

        public async Task DeleteSnapshotAsync(
            Type aggregateType,
            IIdentity identity,
            CancellationToken cancellationToken)
        {

            var queryFilter = new ScanFilter();
            queryFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateName), ScanOperator.Equal, aggregateType.GetAggregateName().Value);
            queryFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateId), ScanOperator.Equal, identity.Value);

            var search = _dynamoDbContext.FromScanAsync<DynamoDbSnapshotDataModel>(new ScanOperationConfig
            {
                Filter = queryFilter,
                Limit = 25,
                ConditionalOperator = ConditionalOperatorValues.And,
            });

            List<DynamoDbSnapshotDataModel> dataModels = new List<DynamoDbSnapshotDataModel>();

            do
            {
                dataModels.AddRange(await search.GetNextSetAsync(cancellationToken));
            } while (!search.IsDone);

            var batchWrite = _dynamoDbContext.CreateBatchWrite<DynamoDbSnapshotDataModel>();
            batchWrite.AddDeleteItems(dataModels);
            await batchWrite.ExecuteAsync(cancellationToken);
        }

        public async Task PurgeSnapshotsAsync(CancellationToken cancellationToken)
        {
            var conditions = new List<ScanCondition>();
            var search = _dynamoDbContext.ScanAsync<DynamoDbSnapshotDataModel>(conditions);

            List<DynamoDbSnapshotDataModel> dataModels = new List<DynamoDbSnapshotDataModel>();

            do
            {
                dataModels.AddRange(await search.GetNextSetAsync(cancellationToken));
            } while (!search.IsDone);

            var batchWrite = _dynamoDbContext.CreateBatchWrite<DynamoDbSnapshotDataModel>();
            batchWrite.AddDeleteItems(dataModels);
            await batchWrite.ExecuteAsync(cancellationToken);
        }

        public async Task PurgeSnapshotsAsync(
            Type aggregateType,
            CancellationToken cancellationToken)
        {
            var queryFilter = new ScanFilter();
            queryFilter.AddCondition(nameof(DynamoDbSnapshotDataModel.AggregateName), ScanOperator.Equal, aggregateType.GetAggregateName().Value);

            var search = _dynamoDbContext.FromScanAsync<DynamoDbSnapshotDataModel>(new ScanOperationConfig
            {
                Filter = queryFilter,
                Limit = 25,
                ConditionalOperator = ConditionalOperatorValues.And,
            });

            List<DynamoDbSnapshotDataModel> dataModels = new List<DynamoDbSnapshotDataModel>();

            do
            {
                dataModels.AddRange(await search.GetNextSetAsync(cancellationToken));
            } while (!search.IsDone);

            var batchWrite = _dynamoDbContext.CreateBatchWrite<DynamoDbSnapshotDataModel>();
            batchWrite.AddDeleteItems(dataModels);
            await batchWrite.ExecuteAsync(cancellationToken);
        }
    }
}
