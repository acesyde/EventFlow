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
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.DynamoDB.Extensions;
using EventFlow.DynamoDB.ValueObjects;

namespace EventFlow.DynamoDB.EventStore
{
    internal class DynamoDbEventSequenceStore : IDynamoDbEventSequenceStore
    {
        private readonly IAmazonDynamoDB _amazonDynamoDb;

        public DynamoDbEventSequenceStore(IAmazonDynamoDB amazonDynamoDb)
        {
            _amazonDynamoDb = amazonDynamoDb;
        }

        public async Task<long> GetNextSequenceAsync(string name, CancellationToken cancellationToken)
        {
            var itemResponse = await _amazonDynamoDb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = DynamoDbExtensions.GetTableName<DynamoDbCounterDataModel>("eventflow.counter"),
                Key = new Dictionary<string, AttributeValue>
                {
                    { nameof(DynamoDbCounterDataModel.Id), new AttributeValue(name)},

                },
                UpdateExpression = "ADD Seq :q",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    {":q", new AttributeValue { N = "1"}}
                }
            }, cancellationToken);

            return Convert.ToInt64(itemResponse.Attributes[nameof(DynamoDbCounterDataModel.Seq)].N);
        }
    }
}