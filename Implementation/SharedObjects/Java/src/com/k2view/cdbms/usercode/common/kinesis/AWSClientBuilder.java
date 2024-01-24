package com.k2view.cdbms.usercode.common.kinesis;

import com.k2view.fabric.common.Util;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;

public class AWSClientBuilder {
    private AWSClientBuilder(){}
    public static KinesisAsyncClientBuilder kinesisClientBuilder(String region) {
        KinesisAsyncClientBuilder kinesisAsyncClientBuilder = KinesisAsyncClient
                .builder()
                .credentialsProvider(DefaultCredentialsProvider.create());
        if (!Util.isEmpty(region)) {
            kinesisAsyncClientBuilder.region(Region.of(region.toLowerCase()));
        }
        return kinesisAsyncClientBuilder;
    }

    public static DynamoDbAsyncClientBuilder dynamoDbClientBuilder(String region) {
        DynamoDbAsyncClientBuilder dynamoDbAsyncClientBuilder = DynamoDbAsyncClient
                .builder()
                .credentialsProvider(DefaultCredentialsProvider.create());
        if (!Util.isEmpty(region)) {
            dynamoDbAsyncClientBuilder.region(Region.of(region.toLowerCase()));
        }
        return dynamoDbAsyncClientBuilder;
    }

    public static CloudWatchAsyncClientBuilder cloudWatchClientBuilder(String region) {
        CloudWatchAsyncClientBuilder cloudWatchAsyncClientBuilder = CloudWatchAsyncClient
                .builder()
                .credentialsProvider(DefaultCredentialsProvider.create());
        if (!Util.isEmpty(region)) {
            cloudWatchAsyncClientBuilder.region(Region.of(region.toLowerCase()));
        }
        return cloudWatchAsyncClientBuilder;
    }
}
