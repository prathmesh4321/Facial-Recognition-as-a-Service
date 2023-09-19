package com.awsCCalpha.project1.webtier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AwsCCAlphaProject1WebtierApplication {

	public static void main(String[] args) {
		SpringApplication.run(AwsCCAlphaProject1WebtierApplication.class, args);

		AwsStubs awsStubs = new AwsStubs();
		AutoScaleService autoScaleService = new AutoScaleService();
		ResponseService responseService = new ResponseService();

		System.out.println("Aws SQS, S3 intializing Now");

		awsStubs.initializeSQSandS3(AWSConstants.AWSSQSInputQueue, AWSConstants.AWSSQSOutputQueue,
				AWSConstants.AWSS3InputBucket, AWSConstants.AWSS3OutputBucket);

		autoScaleService.start();
		System.out.println("INFO - Starting AutoScaleService Thread");

		responseService.start();
		System.out.println("INFO - Starting ResponseService Thread");

	}

}