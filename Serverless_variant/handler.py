import boto3
from boto3 import client as boto3_client
from boto3.dynamodb.conditions import Key, Attr
import face_recognition
import pickle
import os
import urllib
import csv
import logging

AWS_INPUT_BUCKET = "alpha-546proj2"
AWS_OUTPUT_BUCKET = "alpha-546proj2output"
AWS_REGION = 'us-east-1'
AWS_ACCESS_KEY_ID = 'AKIAT2KBUQ4W2BTHS2TL'
AWS_SECRET_KEY = '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'
logger = logging.getLogger()

def getDataFromS3(id):
	s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
	aws_secret_access_key=AWS_SECRET_KEY)
	logger.info("File id ", id)
	temp = id.split('.mp4',1)[0] + '.mp4'
	filepath = f"/tmp/{temp}"
	#Image stored at filepath
	logger.info("Files in directory ", os.listdir("./"))
	s3.download_file(Bucket=AWS_INPUT_BUCKET,Key=temp, Filename=filepath)
	logger.info("File downloaded from s3")
	print('Image downloaded from S3 input bucket')
	# Classify the downloaded image
	return filepath, temp

def insertResultInS3(id, data):
	s3 = boto3.resource('s3',region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
						aws_secret_access_key=AWS_SECRET_KEY)

	temp = id.split('.mp4',1)[0]
	fileName = f'{temp}' + '.csv'
	# tt = data + ' ' + type(data)
	logger.info(data)
	logger.info(type(data))
	print(data[0])
	print(type(data[0]))
	with open('/tmp/{fileName}', 'w', encoding='UTF8') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for key in data[0]:
			if key != 'id':
				writer.writerow([str(key), str(data[0][key])])
	s3.Bucket(AWS_OUTPUT_BUCKET).upload_file('/tmp/{fileName}', fileName)

	
def getDataFromDB(name, fileId):
	# Get Data from Dynamo DB in result

	dynamodb = boto3.resource('dynamodb' ,region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
						aws_secret_access_key=AWS_SECRET_KEY)
	table = dynamodb.Table('FACE_RECOGNITION_DATABASE')
		
	responsedb = table.scan(
			FilterExpression=Attr('name').eq(name)
		)
	items = responsedb['Items']
	logger.info("Item : ",items)
	insertResultInS3(fileId, items)


# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data

def face_recognition_handler(event, context):
	key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
	filepath, fileId = getDataFromS3(key)
	filename = './encoding'
	encoding = open_encoding(filename)
	logger.info(filepath, fileId)
	path = f'/tmp/{fileId}'

	os.system("ffmpeg -i " + str(filepath) + " -r 1 " + str(path) + "image-%3d.jpeg")

	for images in os.listdir('/tmp'):
		if (images.endswith(".jpeg")):
			unknown_image = face_recognition.load_image_file('/tmp/' +images)
			unknown_encoding = face_recognition.face_encodings(unknown_image)[0]
			for i in range(len(encoding['name'])):
				name, enc = encoding['name'][i], encoding['encoding'][i]
				result = face_recognition.compare_faces([enc], unknown_encoding)
				if result[0] ==  True:
					print(name)
					getDataFromDB(name,fileId)
					return

	return

