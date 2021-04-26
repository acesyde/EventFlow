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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using EventFlow.Aggregates;
using EventFlow.Core;
using EventFlow.DynamoDB.ValueObjects;
using EventFlow.EventStores;
using EventFlow.Exceptions;
using EventFlow.Logs;

namespace EventFlow.DynamoDB.EventStore
{
    public class DynamoDbEventPersistence : IEventPersistence
    {
        private readonly ILog _log;
        private readonly IAmazonDynamoDB _dynamoDb;
        private readonly IDynamoDbEventSequenceStore _dynamoDbEventSequenceStore;

        public DynamoDbEventPersistence(ILog log, IAmazonDynamoDB dynamoDb, IDynamoDbEventSequenceStore dynamoDbEventSequenceStore)
        {
            _log = log;
            _dynamoDb = dynamoDb;
            _dynamoDbEventSequenceStore = dynamoDbEventSequenceStore;
        }

        public async Task<AllCommittedEventsPage> LoadAllCommittedEvents(GlobalPosition globalPosition, int pageSize, CancellationToken cancellationToken)
        {
            long startPosition = globalPosition.IsStart
                ? 0
                : long.Parse(globalPosition.Value);

            var query =new QueryOperationConfig
            {
                Limit = pageSize,
                Filter = new QueryFilter("Id", QueryOperator.GreaterThanOrEqual, startPosition)
            };

            var eventDataModels = await DynamoDbEventStoreCollection.FromQueryAsync<DynamoDbEventDataModel>(query)
                .GetNextSetAsync(cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);

            long nextPosition = eventDataModels.Any()
                ? eventDataModels.Max(e => e.Id) + 1
                : startPosition;

            return new AllCommittedEventsPage(new GlobalPosition(nextPosition.ToString()), eventDataModels);
        }

        public async Task<IReadOnlyCollection<ICommittedDomainEvent>> CommitEventsAsync(IIdentity id, IReadOnlyCollection<SerializedEvent> serializedEvents, CancellationToken cancellationToken)
        {
            if (!serializedEvents.Any())
            {
                return new ICommittedDomainEvent[] { };
            }

            var tasks = serializedEvents
                .Select(async (e, i) =>
                {
                    long nextSequence =
                        await _dynamoDbEventSequenceStore.GetNextSequenceAsync(TableName, cancellationToken);

                    return new DynamoDbEventDataModel
                    {
                        Id = nextSequence,
                        AggregateId = id.Value,
                        AggregateName = e.Metadata[MetadataKeys.AggregateName],
                        BatchId = Guid.Parse(e.Metadata[MetadataKeys.BatchId]),
                        Data = e.SerializedData,
                        Metadata = e.SerializedMetadata,
                        AggregateSequenceNumber = e.AggregateSequenceNumber
                    };
                });

            var eventDataModels = (await Task.WhenAll(tasks))
                .OrderBy(m => m.AggregateSequenceNumber)
                .ToList();

            _log.Verbose("Committing {0} events to DynamoDB event store for entity with ID '{1}'", eventDataModels.Count, id);
            try
            {
                var documentBatchWrite = DynamoDbEventStoreCollection
                    .CreateBatchWrite<DynamoDbEventDataModel>();

                documentBatchWrite.AddPutItems(eventDataModels);

                await documentBatchWrite
                    .ExecuteAsync(cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (AmazonDynamoDBException e)
            {
                throw new OptimisticConcurrencyException(e.Message, e);

            }
            return eventDataModels;
        }

        public async Task<IReadOnlyCollection<ICommittedDomainEvent>> LoadCommittedEventsAsync(IIdentity id, int fromEventSequenceNumber, CancellationToken cancellationToken)
        {
            var queryFilter = new QueryFilter("Id", QueryOperator.Equal, id.Value);
            queryFilter.AddCondition("AggregateSequenceNumber", QueryOperator.GreaterThanOrEqual,
                fromEventSequenceNumber);

            return await DynamoDbEventStoreCollection.FromQueryAsync<DynamoDbEventDataModel>(new QueryOperationConfig
            {
                Filter = queryFilter
            })
            .GetNextSetAsync(cancellationToken)
            .ConfigureAwait(continueOnCapturedContext: false);
        }

        public async Task DeleteEventsAsync(IIdentity id, CancellationToken cancellationToken)
        {
            await DynamoDbEventStoreCollection.DeleteAsync<DynamoDbEventDataModel>(id.Value, cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);

            _log.Verbose("Deleted entity with ID '{0}' by deleting all of its {1} events", id, 1);
        }
        private IDynamoDBContext DynamoDbEventStoreCollection => new DynamoDBContext(_dynamoDb);
    }
}
