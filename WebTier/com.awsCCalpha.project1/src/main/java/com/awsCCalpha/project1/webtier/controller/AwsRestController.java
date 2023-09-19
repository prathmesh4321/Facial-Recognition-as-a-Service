package com.awsCCalpha.project1.webtier.controller;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.awsCCalpha.project1.webtier.ResponseService;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.awsCCalpha.project1.webtier.AWSConstants;
import com.awsCCalpha.project1.webtier.s3.AwsS3Operations;
import com.awsCCalpha.project1.webtier.sqs.SQSOperations;

@RestController
public class AwsRestController {

	private ResponseService responseService = new ResponseService();
	private SQSOperations sqsOperations = new SQSOperations();
	private AwsS3Operations s3Operations = new AwsS3Operations();

	@PostMapping(value = "/recognizeImage")
	public String postCall(@RequestParam(name = "myfile", required = true) MultipartFile[] multipartFiles)
			throws AmazonServiceException, SdkClientException, Exception {

		Set<String> uniqueList = modifyImage(multipartFiles);
		sqsOperations.generateQueue(AWSConstants.AWSSQSOutputQueue);

		Map<String, String> results = new TreeMap<>();
		for (String imgReceivedName : uniqueList) {

			String[] response = responseService.getResultFromResponseService(imgReceivedName);
			results.put(response[0], response[1]);
		}

		StringBuilder finalResponseString = new StringBuilder();
		for (String key : results.keySet()) {
			finalResponseString.append(results.get(key));
		}

		return finalResponseString.toString();
	}

	private Set<String> modifyImage(MultipartFile[] multipartFiles)
			throws AmazonServiceException, SdkClientException, Exception {

		Set<String> list = new HashSet<>();

		for (MultipartFile multipartFile : multipartFiles) {
			String s = System.currentTimeMillis() + "-" + multipartFile.getOriginalFilename().replace(" ", "_");
			System.out.println("INFO - Input File Name: " + s);

			s3Operations.putImageS3(multipartFile, s);
			System.out.println("INFO - " + s + " Uploaded to S3");

			sqsOperations.inputInSQS(s, AWSConstants.AWSSQSInputQueue, 0);
			System.out.println("INFO - " + s + " Enqueued in SQS");

			list.add(s);
		}

		return list;
	}
}
