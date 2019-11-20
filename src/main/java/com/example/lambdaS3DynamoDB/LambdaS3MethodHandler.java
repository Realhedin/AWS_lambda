package com.example.lambdaS3DynamoDB;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Stream;

/**
 * This Lambda get object from S3 Bucket,
 * and calculate average from numbers in CSV file.
 * Put gradesCSV.csv file into S3 Bucket.
 * Test data should be created from S3 Put template.
 * Changing information about bucket, arn and key.
 * Plus, IAM role should allow S3 read.
 *
 */
public class LambdaS3MethodHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    public LambdaS3MethodHandler() {
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);

        //Get object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();

        try {
            S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
            String contentType = response.getObjectMetadata().getContentType();

            //get content of object (CSV file)
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.getObjectContent()));

            //calculate grade
            String csvOutput;
            double average =0;
            while ((csvOutput = bufferedReader.readLine()) != null) {
                String[] split = csvOutput.split(",");
                average = Stream.of(split).mapToInt(Integer::parseInt).average().getAsDouble();
            }
            return String.valueOf(average);
        } catch (IOException e) {
            e.printStackTrace();
            context.getLogger().log(String.format("Error getting object %s from bucket %s", key, bucket));
            return e.toString();
        }

    }
}
