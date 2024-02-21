package com.agrawal.KinesisProducer.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class AwsKinesisClient {
    public static final String AWS_ACCESS_KEY = "aws.accessKeyId";
    public static final String AWS_SECRET_KEY = "aws.secretKey";

    static {
        System.getProperty(AWS_ACCESS_KEY, "");
        System.getProperty(AWS_SECRET_KEY, "");
    }

    public static AmazonKinesis getKinesisClient(){
        return AmazonKinesisClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
    }
}
