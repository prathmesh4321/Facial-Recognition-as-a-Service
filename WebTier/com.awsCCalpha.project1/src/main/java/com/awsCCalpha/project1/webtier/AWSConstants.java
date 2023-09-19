package com.awsCCalpha.project1.webtier;

import com.amazonaws.regions.Regions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AWSConstants {

	public static final String AWSAccessKeyId = "AKIA4QRG3KYYTGQXOYB6";

	public static final String AWSSecretKey = "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";

	public static final String AmazonMachineImageId = "ami-01b410219a63a78ee";

	public static final String EC2KeyPair = "CC-Class";

	public static final String AWSS3InputBucket = "input-bucket-alpha";

	public static final String AWSS3OutputBucket = "output-bucket-alpha";

	public static final String AWSSQSInputQueue = "input-queue-alpha.fifo";

	public static final String AWSSQSOutputQueue = "output-queue-alpha.fifo";

	public static final String EC2InstanceType = "t2.micro";

	public static final Regions AWSRegion = Regions.US_EAST_1;

	public static final String TotalMsgInQueue = "ApproximateNumberOfMessages";

	public static final String TotalMsgInSQSNotVisible = "ApproximateNumberOfMessagesNotVisible";

	public static final Integer MaximumNumberOfAppTierInstances = 20;

	public static final List<String> SQSMetrics = new ArrayList<String>(
			Arrays.asList(TotalMsgInQueue, TotalMsgInSQSNotVisible));

	public static final String EC2TagKey = "Name";

	public static final String EC2TagValue = "App-Instance";

	public static final String ResourceInstance = "instance";

	public static final String AppTierScript = "#!/bin/bash" + "\n" + "cd /home/ubuntu" + "\n"
			+ "sudo -i -u ubuntu bash << EOF" + "\n" + "chmod +x App.py" + "\n" + "python3 App.py" + "\n" + "EOF";

	public static final Integer TimeOutTime = 20;

}
