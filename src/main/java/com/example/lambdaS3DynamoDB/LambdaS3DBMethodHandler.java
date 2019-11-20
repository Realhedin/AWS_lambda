package com.example.lambdaS3DynamoDB;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
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
 * Put gradesCSV.csv file into S3 Bucket.
 * Create DynamoDB instance and table "Students" with "StudentID" key.
 *
 * This Lambda get object from S3 Bucket,
 * and calculate average from numbers in CSV file.
 * After that, it saves Student item into DynamoDB.
 *
 * Test data should be created from S3 Put template.
 * Changing information about bucket, arn and key.
 * Plus, IAM role should allow S3 read.
 */
public class LambdaS3DBMethodHandler implements RequestHandler<S3Event, String> {
    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    private AmazonDynamoDB clientDB = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_EAST_2)
            .build();
    private DynamoDB dynamoDB = new DynamoDB(clientDB);

    private static String tableName = "Students";

    public LambdaS3DBMethodHandler() {
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
            PutItemOutcome dynamoItem = null;
            while ((csvOutput = bufferedReader.readLine()) != null) {
                String[] split = csvOutput.split(",");
                double average = Stream.of(split).skip(1).mapToInt(Integer::parseInt).average().getAsDouble();
                dynamoItem = createDynamoItem(Integer.parseInt(split[0]), average);
            }
            return (dynamoItem == null) ? "null" : dynamoItem.toString() ;
        } catch (IOException e) {
            e.printStackTrace();
            context.getLogger().log(String.format("Error getting object %s from bucket %s", key, bucket));
            return e.toString();
        }

    }

    private PutItemOutcome createDynamoItem(int studentId, double average) {
        Table table = dynamoDB.getTable(tableName);

        try {
            Item item = new Item().withPrimaryKey("StudentID", studentId).withDouble("Grade", average);
            return table.putItem(item);
        } catch (Exception e) {
            System.err.println("Create item failed");
            System.err.println(e.getMessage());
        }
        return null;
    }
}
