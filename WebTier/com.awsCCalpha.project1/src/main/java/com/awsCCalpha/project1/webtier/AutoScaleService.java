package com.awsCCalpha.project1.webtier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.util.Base64;
import com.awsCCalpha.project1.webtier.sqs.SQSOperations;

public class AutoScaleService extends Thread {

	private SQSOperations sqsOperations = new SQSOperations();
	private AwsStubs awsStubs = new AwsStubs();

	@Override
	public void run() {
		this.autoScaleInstances();
	}

	private PriorityQueue<ec2check> pq = new PriorityQueue<>(new Comparator<ec2check>() {
		@Override
		public int compare(ec2check ec21, ec2check ec22) {
			if (ec21.b == false && ec22.b == false) {
				if (ec21.n <= ec22.n) {
					return -1;
				} else {
					return 1;
				}
			} else if (ec21.b == true && ec22.b == false) {
				return 1;
			} else if (ec21.b == false && ec22.b == true) {
				return -1;
			} else {
				if (ec21.n <= ec22.n) {
					return -1;
				} else {
					return 1;
				}
			}
		}
	});

	private int check = 0;

	public void autoScaleInstances() {
		for (;;) {
			if (check == 0) {
				check = 1;
				for (int i = 0; i < 20; i++) {
					pq.add(new ec2check(i, false));
				}
			}

			int msgsPendinginQueue = sqsOperations.countNumberOfMessagesFromQueue(AWSConstants.AWSSQSInputQueue);
			int instanceRunningEC2 = noOfec2RunningNow();
			int ec2toRunNow = ec2CountToRunNow(instanceRunningEC2, msgsPendinginQueue);

			System.out.println("INFO - Will Instantiate " + ec2toRunNow + " Instances");

			if (ec2toRunNow == 1) {
				initializeAndStartInstance(AWSConstants.AmazonMachineImageId, AWSConstants.EC2InstanceType, 1, 1);
			} else if (ec2toRunNow > 1) {
				initializeAndStartInstance(AWSConstants.AmazonMachineImageId, AWSConstants.EC2InstanceType,
						ec2toRunNow - 1, ec2toRunNow);
			}

			try {
				Thread.sleep(2500);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void initializeAndStartInstance(String ami, String instType, Integer minInstances, Integer maxIinstances) {
		try {
			Integer numOfInstances = noOfec2RunningNow();
			if (numOfInstances + maxIinstances > AWSConstants.MaximumNumberOfAppTierInstances) {
				if (AWSConstants.MaximumNumberOfAppTierInstances - numOfInstances > 0) {
					maxIinstances = AWSConstants.MaximumNumberOfAppTierInstances - numOfInstances;
					minInstances = maxIinstances == 1 ? 1 : maxIinstances - 1;
				} else {
					return;
				}
			}

			RunInstancesRequest instancesRequest = getAwsAllTagsAndRunInstances(ami, instType, minInstances,
					maxIinstances);
			awsStubs.ec2Client().runInstances(instancesRequest);

		} catch (Exception e) {
			System.out.println("EXCEPTION - In Initializing EC2 Instance");
			e.printStackTrace();
			return;
		}
	}

	private RunInstancesRequest getAwsAllTagsAndRunInstances(String id, String instType, Integer min, Integer max)
			throws Exception {
		try {
			Collection<Tag> tags = new ArrayList<>();
			TagSpecification tagSpecification = new TagSpecification();
			Tag t = new Tag();
			t.setKey(AWSConstants.EC2TagKey);
			int n = (pq.poll().n) % 20;
			t.setValue(AWSConstants.EC2TagValue + "-" + n);
			pq.add(new ec2check(n, true));
			tags.add(t);
			tagSpecification.setResourceType(AWSConstants.ResourceInstance);
			tagSpecification.setTags(tags);
			RunInstancesRequest instancesRequest = new RunInstancesRequest().withImageId(id).withInstanceType(instType)
					.withMinCount(min).withMaxCount(max).withKeyName(AWSConstants.EC2KeyPair)
					.withTagSpecifications(tagSpecification)
					.withUserData(new String(Base64.encode(AWSConstants.AppTierScript.getBytes("UTF-8")), "UTF-8"));

			return instancesRequest;
		} catch (Exception e) {
			throw e;
		}

	}

	public Integer noOfec2RunningNow() {
		DescribeInstanceStatusRequest instanceStatusRequest = new DescribeInstanceStatusRequest();
		instanceStatusRequest.setIncludeAllInstances(true);
		DescribeInstanceStatusResult describeInstances = awsStubs.ec2Client()
				.describeInstanceStatus(instanceStatusRequest);
		List<InstanceStatus> instanceStatusList = describeInstances.getInstanceStatuses();

		int num = 0;
		for (InstanceStatus is : instanceStatusList) {
			if (is.getInstanceState().getName().equals(InstanceStateName.Running.toString())
					|| is.getInstanceState().getName().equals(InstanceStateName.Pending.toString()))
				num++;
		}

		Filter filter = new Filter("instance-state-name");
		filter.withValues(InstanceStateName.ShuttingDown.toString());
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		DescribeInstancesResult result = awsStubs.ec2Client().describeInstances(request.withFilters(filter));

		List<Reservation> reservations = result.getReservations();

		for (Reservation reservation : reservations) {
			List<Instance> instances = reservation.getInstances();

			for (Instance instance : instances) {

				Tag abc = instance.getTags().get(0);
				String[] def = abc.getValue().split("App-Instance-");

				if (def.length > 1) {
					pq.remove(new ec2check(Integer.parseInt(def[1]), true));
					pq.add(new ec2check(Integer.parseInt(def[1]), false));
				} else {
					pq.remove(new ec2check(Integer.parseInt(def[0]), true));
					pq.add(new ec2check(Integer.parseInt(def[0]), false));
				}
			}
		}

		return num - 1;
	}

	public class ec2check {
		int n;
		boolean b;

		ec2check(int n1, boolean b1) {
			n = n1;
			b = b1;
		}

		ec2check() {
		};

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null || obj.getClass() != this.getClass())
				return false;
			ec2check cache = (ec2check) obj;
			return n == cache.n && b == cache.b;
		}
	}

	private Integer ec2CountToRunNow(Integer numberOfInstances, Integer numberOfMessagesFromQueue) {
		Integer countToRun = 0;
		if (numberOfInstances < numberOfMessagesFromQueue) {
			if (numberOfMessagesFromQueue - numberOfInstances < AWSConstants.MaximumNumberOfAppTierInstances) {
				countToRun = numberOfMessagesFromQueue - numberOfInstances;
			} else {
				countToRun = AWSConstants.MaximumNumberOfAppTierInstances - numberOfInstances;
			}
		}

		return countToRun;
	}

}
