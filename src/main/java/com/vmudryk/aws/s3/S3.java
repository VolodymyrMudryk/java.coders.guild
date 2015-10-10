package com.vmudryk.aws.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.File;

/**
 * Created by Volodymyr on 10.10.2015.
 */
public class S3 {

    private static final String BUCKET_NAME = "atest12";
    private AmazonS3Client s3client;

    public S3(AWSCredentials awsCredentials) {
        s3client = new AmazonS3Client(awsCredentials);
    }

    public void store(File file) {
        PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, file.getName(), file);
        s3client.putObject(putObjectRequest);
    }

    public S3Object get(String path) {
        return s3client.getObject(BUCKET_NAME, path);
    }

    public void delete(String path) {
        s3client.deleteObject(BUCKET_NAME, path);
    }
}
