Based on tutorial:
https://examples.javacodegeeks.com/software-development/amazon-aws/tutorial-use-aws-lambda-s3-real-time-data-processing/

To make it work, following actions need to be done:
1) create new Lambda for each java file into AWS

2) create S3 bucket (LambdaS3MethodHandler.java, LambdaS3DBMethodHandler.java)

3) Create DynamoDB instance (LambdaS3DBMethodHandler.java) 