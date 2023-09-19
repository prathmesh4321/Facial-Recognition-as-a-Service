import boto3
from botocore.exceptions import ClientError
from ec2_metadata import ec2_metadata
import time
import constants
import subprocess

instanceId = ""
image_classified_name = ""

# Check if output queue exists if not create output queue
def checkOutputQueueExist():
	sqs = boto3.resource('sqs', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	try:
		response = sqs.get_queue_by_name(QueueName=constants.Constants.SQS_OUTPUT_QUEUE)
	except BaseException as e:
		print(e)
		createOutputQueue()

# Create output queue if not exists
def createOutputQueue():
	sqs = boto3.resource('sqs', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	queue = sqs.create_queue(QueueName=constants.Constants.SQS_OUTPUT_QUEUE, Attributes={'FifoQueue': 'true', 'ContentBasedDeduplication':'true'})

# Check if output bucket exists if not create output bucket
def checkOutputBucketExist():
	client = boto3.client('s3', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	response = client.list_buckets()
	for bucket in response['Buckets']:
		if(bucket['Name'] == constants.Constants.S3_OUPUT_BUCKET):
			return
	createOutputBucket()

# Create output bucket if not exists
def createOutputBucket():
	client = boto3.resource('s3', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	bucket = client.Bucket(constants.Constants.S3_OUPUT_BUCKET)
	response = bucket.create()

# Poll SQS Input Queue for message
def pollInputSQS():
	sqs = boto3.client('sqs', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	queueUrl = sqs.get_queue_url(QueueName=constants.Constants.SQS_INPUT_QUEUE)
	msgQueue = sqs.receive_message(QueueUrl=queueUrl["QueueUrl"])
	
	# If no Messages in SQS Queue return None
	if "Messages" not in msgQueue.keys():
		return None
	
	que_length=len(msgQueue['Messages'])
	if que_length>0:
		# Retreive image name from body of first message in queue
		message=msgQueue['Messages'][0]['Body']
		# Retreive reciept handle to delete message on read
		recieptHandle = msgQueue['Messages'][0]['ReceiptHandle']
		if message is not None:
			image_name = message
			# Delete message from SQS Input Queue
			sqs.delete_message(QueueUrl=queueUrl["QueueUrl"], ReceiptHandle=recieptHandle)
			return image_name
		else:
			return None
	else:
		return None

# Download image from S3 Input Bucket using imageId recieved from SQS Input Queue
def getImageFromS3(ImageId):
	s3 = boto3.client('s3', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	image = ImageId.split('-')[-1]
	filepath = "./Images/"+image
	#Image stored at filepath
	s3.download_file(Bucket=constants.Constants.S3_INPUT_BUCKET,Key=ImageId, Filename=filepath)
	print('Image downloaded from S3 input bucket')
	# Classify the downloaded image
	classifyImage(image, filepath)

# Use image classifier provided to classify the image
def classifyImage(image, filepath):
	result = (subprocess.run(['python3', 'image_classification.py',
				filepath], capture_output=True)).stdout.decode().strip()
	# Only require the classification result
	image_classification = result.split(',')[-1]
	print('Image classified as : ', result)
	#Insert the result in SQS Output Queue
	insertResultInQueue(image, image_classification)
	#Insert the result in S3 Output Bucket
	insertResultInS3(image, image_classification)

# Insert Result of classification in SQS Output Queue
def insertResultInQueue(ImageId, image_classified_name):
	# Fetch sqs queue resource by name
	sqs = boto3.resource('sqs', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
						aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	output_queue = sqs.get_queue_by_name(QueueName=constants.Constants.SQS_OUTPUT_QUEUE)
	
	# Fetch sqs client 
	sqsclient = boto3.client('sqs', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	queueUrl = sqsclient.get_queue_url(QueueName=constants.Constants.SQS_OUTPUT_QUEUE)
	
	body=''+ ImageId + '####' + image_classified_name + ''
	
	# Send message to SQS Queue in MessageBody and MessageAttributes
	result = output_queue.send_message(
		QueueUrl=queueUrl["QueueUrl"],
		MessageAttributes={
			"image_name": {
				'DataType': 'String',
				'StringValue': str(ImageId)
			},
			"image_classified_name": {
				'DataType': 'String',
				'StringValue': str(image_classified_name)
			}
		},
		MessageBody=str(body),
		MessageGroupId="ccgroupalphaoutputqueuemsgid"
	)
	print("Message sent to SQS Output Queue", result)


# Insert result of Classification in S3 Output Bucket
def insertResultInS3(ImageId, image_classified_name):
	s3 = boto3.resource('s3',region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,
						aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
	
	key_name = ImageId.split('.')[0]
	fileName = f'{key_name}'
	object = s3.Object(constants.Constants.S3_OUPUT_BUCKET, fileName)
	result = object.put(Body=image_classified_name)
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:
		print('File Uploaded Successfully')
	else:
		print('File Not Uploaded')
	


def terminateEC2():
	try:
		ec2 = boto3.client('ec2', region_name=constants.Constants.AWS_REGION, aws_access_key_id=constants.Constants.AWS_ACCESS_KEY_ID,aws_secret_access_key=constants.Constants.AWS_SECRET_KEY)
		response = ec2.terminate_instances(InstanceIds=[instanceId], DryRun=False)
		print(response)
	except ClientError as e:
		print(e)

def runAppTier():
	startTime = time.time()
	while True:
		# We poll SQS Input queue for message and receive the first message in FIFO order else None
		ImageId=pollInputSQS()
		print("polling sqs input queue")
		
		if ImageId is not None:
			getImageFromS3(ImageId)
			startTime = time.time()

		# If SQS Input queue remains empty for 60 seconds we scale in by terminating this EC2 instance
		if time.time() - startTime > 60:
			terminateEC2()


if __name__ == '__main__':
	instanceId = ec2_metadata.instance_id
	checkOutputQueueExist()
	checkOutputBucketExist()
	runAppTier()
	
