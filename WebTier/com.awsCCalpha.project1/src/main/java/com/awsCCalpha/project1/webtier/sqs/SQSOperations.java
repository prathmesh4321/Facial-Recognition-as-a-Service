package com.awsCCalpha.project1.webtier.sqs;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.awsCCalpha.project1.webtier.ResponseService;
import com.awsCCalpha.project1.webtier.AWSConstants;

public class SQSOperations {

	public AmazonSQS sqsClient() {
		return AmazonSQSClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(
						new BasicAWSCredentials(AWSConstants.AWSAccessKeyId, AWSConstants.AWSSecretKey)))
				.withRegion(AWSConstants.AWSRegion).build();
	}

	public String[] accessResponseQueue(String s) {
		for(;;) {
			try {
				s = s.substring(s.indexOf('-') + 1, s.length());

				if (ResponseService.response.containsKey(s)) {
					String output = ResponseService.response.get(s);

					ResponseService.response.remove(s);
					return new String[] { s, "The uploaded image " + s + " is " + output };

				} else {
					try {
						Thread.sleep(6000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				System.out.println("ERROR - While accessing Response Queue: " + e.getMessage());
				try {
					Thread.sleep(6000);
				} catch (Exception o) {
					o.printStackTrace();
				}
			}
		}
	}

	public Integer countNumberOfMessagesFromQueue(String s) {
		String qLink = null;

		try {
			qLink = sqsClient().getQueueUrl(s).getQueueUrl();
		} catch (Exception e) {
			generateQueue(s);
		}
		qLink = sqsClient().getQueueUrl(s).getQueueUrl();
		Map<String, String> map = sqsClient()
				.getQueueAttributes(new GetQueueAttributesRequest(qLink, AWSConstants.SQSMetrics)).getAttributes();
		return Integer.parseInt((String) map.get(AWSConstants.TotalMsgInQueue));
	}

	public void generateQueue(String s) {
		try {
			sqsClient().createQueue(new CreateQueueRequest().withQueueName(s)
					.addAttributesEntry(QueueAttributeName.FifoQueue.toString(), Boolean.TRUE.toString())
					.addAttributesEntry(QueueAttributeName.ContentBasedDeduplication.toString(),
							Boolean.TRUE.toString()));
		} catch (Exception e) {
			System.out.println("ERROR - In Generating Queue: " + e.getMessage());
		}
	}

	public void inputInSQS(String s, String sqsName, int time) {
		String qLink = null;

		try {
			qLink = sqsClient().getQueueUrl(sqsName).getQueueUrl();
		} catch (Exception e) {
			generateQueue(sqsName);
		}

		qLink = sqsClient().getQueueUrl(sqsName).getQueueUrl();
		sqsClient().sendMessage(new SendMessageRequest().withQueueUrl(qLink)
				.withMessageGroupId(UUID.randomUUID().toString()).withMessageBody(s).withDelaySeconds(0));
	}

	public List<Message> getMessageFromSQS(String s, Integer timeout, Integer wait, Integer numMsg) {
		try {
			ReceiveMessageRequest messageRequest = new ReceiveMessageRequest(sqsClient().getQueueUrl(s).getQueueUrl());
			messageRequest.setMaxNumberOfMessages(numMsg);
			messageRequest.setVisibilityTimeout(timeout);
			messageRequest.setWaitTimeSeconds(wait);
			ReceiveMessageResult receiveMessageResult = sqsClient().receiveMessage(messageRequest);
			List<Message> messageList = receiveMessageResult.getMessages();
			if (messageList.isEmpty()) {
				System.out.println("INFO - Message List Empty");
				return null;
			}
			return messageList;
		} catch (Exception e) {
			System.out.println("ERROR - Getting Messages: " + e.getMessage());

			System.out.println("INFO - Thread Sleeping for 10 seconds");
			try {
				Thread.sleep(6000);
			} catch (Exception p) {
				System.out.println("ERROR - In Thread Sleep");
			}
			return null;
		}
	}

	public void removeMessageFromQueue(Message m, String s) {
		try {
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(
					sqsClient().getQueueUrl(s).getQueueUrl(), m.getReceiptHandle());
			sqsClient().deleteMessage(deleteMessageRequest);
		} catch (Exception e) {
			System.out.println("EXCEPTION - In Removing Messages: " + e.getMessage());
		}
	}

}
