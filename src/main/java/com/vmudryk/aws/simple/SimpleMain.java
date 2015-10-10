package com.vmudryk.aws.simple;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 * Created by Volodymyr on 07.10.2015.
 */
public class SimpleMain {

    private static final String ACCESS_KEY = "";
    private static final String SECRET_KEY = "";

    public static void main(String[] args) {
        AWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);


    }

}
