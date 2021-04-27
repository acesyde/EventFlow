using System.Reflection;
using Amazon.DynamoDBv2.DataModel;

namespace EventFlow.DynamoDB.Extensions
{
    internal static class DynamoDbExtensions
    {
        public static string GetTableName<T>(string defaultValue = null)
        {
            var attribute = typeof(T).GetCustomAttribute(typeof(DynamoDBTableAttribute)) as DynamoDBTableAttribute;
            return attribute?.TableName ?? defaultValue;
        }
    }
}
