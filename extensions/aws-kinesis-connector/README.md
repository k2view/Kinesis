# Kinesis Data Streams Connector

## Prerequisites   
- Make sure you have all the access roles to DynamoDB, since the consuming is based on the KCL library which makes use of DynamoDB to track the "offsets" for each stream/application. For more info on KCL and how it works, refer to https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html
    - Note: When you consume a stream with a new application name, expect some delay in receiving the messages, due to the time needed to create and initialize the relevant DynamoDB table. 

## Introduction

This extension utilizes the AWS Java SDK for publishing to Kinesis, and the KCL library for consuming from it.  

It provides two main classes:
1. KinesisIoProvider - The integration point with Fabric.
2. KinesisSession - The core functionality. This class creates and maintains the lifecycle of a KinesisPublisher/KinesisConsumer based on the actor creating it.  

## Authentication: 
Any of the 6 methods mentioned in [Default Credential Provider Chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) is natively supported. 
As for what is recommended, 6th is preferred when running on EC2 instances, and 5th in the case of container.

## How to Use
1. Either create a new Kinesis custom interface, or reuse the example interface provided:
   - Set the IoProvider Function to "kinesisIoProvider"
   - Set the Tech Category to "PubSub" (in order to let the relevant actors find it).
   - Specify the AWS region in the "Host" section (If you don’t explicitly set it, the AWS SDK consults the [Default Region Provider Chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment) to try and determine the region to use).
3. Use the existing Broadway actors Publish/Subscribe/SubscribeBatch/SubscribeWithMetadata, with this mapping of inputs to the Kinesis terminologies:
    - Subscribe:
        - interface = {The created interface with kinesisIoProvider}
        - topic = {kinesis stream}
        - group_id = {application name}
        - partitions = {shards ids separated by comma, e.g. "shardId-000000000001,shardId-000000000002"}
        - max_poll_records = same
        - poll_timeout = same
    - Publish:
        - interface = {The created interface with kinesisIoProvider}
        - topic = {kinesis stream}
        - key = {kinesis partition key}
        - partition = {explicit hash key (optional)}
        - correlation_id = unused
        - message = {message}
        - transaction_mode = unused (In reality it is asynchronous when in a transaction, otherwise synchronous)

## Configuring the connector ##
The interface must be of type Custom. In the "Data" section of the interface, you can provide a JSON with 2 keys, "consumer" & "publisher"; The value of each such key is another JSON object.  

**Configurable parameters**
- "publisher":
  - MAX_RETRIES: Default is 3. Retries may be helpful when throttling occurs. (https://repost.aws/knowledge-center/kinesis-data-stream-throttling-errors)
  - RECORDS_PER_REQUEST: Default and maximum is 500. The number of records in each PutRecords request. This is valid only for the transaction case, as in auto-commit records are published one by one. You may need to decrease this param if the records being written are large. (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
- "consumer":
  - CONSUMER_TYPE: "SHARED_THROUGHPUT" or "ENHANCED_FAN_OUT". Default is "SHARED_THROUGHPUT".
  - INITIAL_POSITION_IN_STREAM: "LATEST" or "TRIM_HORIZON". Default is "LATEST". (https://javadoc.io/static/software.amazon.kinesis/amazon-kinesis-client/2.5.2/software/amazon/kinesis/common/InitialPositionInStream.html)
  - CLOUD_WATCH_METRICS: true or false. Default is false. (https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-kcl.html)

## Transactions Behavior ##
- Publish:
    - **Not a real transaction, there's no way in Kinesis to delete records or rollback.** 
    - In the AWS SDK, there are 2 API calls, PutRecord and PutRecords. In transactions, the data is published in batches using async PutRecords API calls, while in auto-commit the records are sent one-by-one using PutRecord API, synchronously. **It is recommended to use the transaction version since it makes less API calls.**
- Subscribe:
    - Using the KCL lease checkpoint mechanism, in case of a transaction, we checkpoint (or in other words - commit) once Broadway commits. In case of auto commit, we checkpoint once the next batch is polled.
    - Note: Checkpoints updates the relevant DynamoDB table for the consuming application. Therefor, once KCL runs again, it will fetch the checkpoint from that DynamoDB table and continue consuming from the last checkpoint. 

### License
[Open license file](/api/k2view/aws-kinesis-connector/0.0.1/file/LICENSE.txt)

