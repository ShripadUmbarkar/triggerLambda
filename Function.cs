using Amazon.Lambda.Core;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.S3;
using Amazon.Lambda.S3Events;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace TriggerLambda;

public class Function
{
    static readonly AmazonDynamoDBClient dynamoClient = new AmazonDynamoDBClient();
    static readonly AmazonS3Client s3Client = new AmazonS3Client();

    public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
    {
        foreach (var record in s3Event.Records)
        {
            var s3 = record.S3;
            var bucketName = s3.Bucket.Name;
            var objectKey = s3.Object.Key;
            var size = s3.Object.Size;
            var eventName = record.EventName;
            var eventTime = record.EventTime;

            var dynamoTable = Table.LoadTable(dynamoClient, "newtable");
            var document = new Document
            {
                ["unique"] = Guid.NewGuid().ToString(),
                ["Bucket"] = bucketName,
                ["Object"] = objectKey,
                ["Size"] = size,
                ["Event"] = eventName,
                ["EventTime"] = eventTime
            };

            await dynamoTable.PutItemAsync(document);
        }
    }
}
