package com.awsCCalpha.project1.webtier.s3;

import java.io.File;
import java.io.FileOutputStream;

import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.awsCCalpha.project1.webtier.AWSConstants;

public class AwsS3Operations {

	public AmazonS3 s3Client() {
		return AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(
						new BasicAWSCredentials(AWSConstants.AWSAccessKeyId, AWSConstants.AWSSecretKey)))
				.withRegion(AWSConstants.AWSRegion).build();
	}

	public void putImageS3(MultipartFile multipartFiles, String imgName)
			throws AmazonServiceException, SdkClientException, Exception {
		try {
			File imgFile = modifyImgFile(multipartFiles);

			if (!s3Client().doesBucketExistV2(AWSConstants.AWSS3InputBucket))
				s3Client().createBucket(AWSConstants.AWSS3InputBucket);

			s3Client().putObject(new PutObjectRequest(AWSConstants.AWSS3InputBucket, imgName, imgFile));

			imgFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public File modifyImgFile(MultipartFile multipartFile) {
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
