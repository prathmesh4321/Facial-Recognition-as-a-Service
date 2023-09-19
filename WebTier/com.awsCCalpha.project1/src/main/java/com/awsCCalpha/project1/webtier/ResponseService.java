package com.awsCCalpha.project1.webtier;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.services.sqs.model.Message;
import com.awsCCalpha.project1.webtier.sqs.SQSOperations;

public class ResponseService extends Thread {

	private SQSOperations sqsOperations = new SQSOperations();

	@Override
	public void run() {
		this.receivedResponse();
	}

	public static Map<String, String> response = new HashMap<>();

	public void receivedResponse() {
		for(;;) {
			List<Message> list = null;
			try {
				list = sqsOperations.getMessageFromSQS(AWSConstants.AWSSQSOutputQueue, 20, AWSConstants.TimeOutTime,
						10);
				if (list != null) {
					try {
						for (Message message : list) {
							messageToResult(message);
						}
					} catch (Exception e) {
						System.out.println("ERROR - Mapping the Output Queue Messages: " + e.getMessage());
					}
				}
			} catch (Exception e) {
				System.out.println("ERROR - No Messages: " + e.getMessage());

				System.out.println("INFO - Thread Sleeping for 10 seconds");
				try {
					Thread.sleep(10000);
				} catch (Exception p) {
					System.out.println("ERROR - In Thread Sleep");
				}
			}
		}
	}

	private void messageToResult(Message message) {
		String[] classificationResult = null;
		System.out.println("INFO - MESSAGE BODY RECEIVED");

		classificationResult = message.getBody().split("####");
		response.put(classificationResult[0], classificationResult[1]);

		sqsOperations.removeMessageFromQueue(message, AWSConstants.AWSSQSOutputQueue);
	}

	public String[] getResultFromResponseService(String s) {
		String[] res = sqsOperations.accessResponseQueue(s);
		return res;
	}

	public File modifyMultiParts(MultipartFile multipartFile) {
		try {
			File f = new File(multipartFile.getOriginalFilename());
			FileOutputStream fos = new FileOutputStream(f);
			fos.write(multipartFile.getBytes());
			fos.close();
			return f;
		} catch (Exception e) {
			return null;
		}
	}

}
