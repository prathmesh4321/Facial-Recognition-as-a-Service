package com.awsCCalpha.project1.webtier;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.awsCCalpha.project1.webtier.s3.AwsS3Operations;
import com.awsCCalpha.project1.webtier.sqs.SQSOperations;

public class AwsStubs {

	private SQSOperations sqsOperations = new SQSOperations();
	private AwsS3Operations s3Operations = new AwsS3Operations();

	public AmazonEC2 ec2Client() {
		return AmazonEC2ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(
						new BasicAWSCredentials(AWSConstants.AWSAccessKeyId, AWSConstants.AWSSecretKey)))
				.withRegion(AWSConstants.AWSRegion).build();
	}

	public void initializeSQSandS3(String inputQueue, String outputQueue, String inputBucket, String outputBucket) {

		try {
			sqsOperations.sqsClient().getQueueUrl(inputQueue).getQueueUrl();
		} catch (Exception e) {
			sqsOperations.generateQueue(inputQueue);
		}

		try {
			sqsOperations.sqsClient().getQueueUrl(outputQueue).getQueueUrl();
		} catch (Exception e) {
			sqsOperations.generateQueue(outputQueue);
		}

		try {
			if (!s3Operations.s3Client().doesBucketExistV2(inputBucket))
				s3Operations.s3Client().createBucket(inputBucket);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (!s3Operations.s3Client().doesBucketExistV2(outputBucket))
				s3Operations.s3Client().createBucket(outputBucket);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
