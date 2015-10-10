package com.vmudryk.aws.cloudformation;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.amazonaws.services.cloudformation.model.DescribeStacksRequest;
import com.amazonaws.services.cloudformation.model.DescribeStacksResult;

/**
 * Created by Volodymyr on 07.10.2015.
 */
public class CloudFormation {

    private static final String STACK_NAME = "SomeStack";

    public void initProperties(AWSCredentials credentials) {
        DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest();
        describeStacksRequest.setStackName(STACK_NAME);

        AmazonCloudFormationClient cloudFormationClient = new AmazonCloudFormationClient(credentials);
        DescribeStacksResult describeStacksResult = cloudFormationClient.describeStacks(describeStacksRequest);

        describeStacksResult.getStacks().forEach(stack -> {
            stack.getOutputs().forEach(output -> {
                String resourceName = output.getOutputKey();
                String resourceValue = output.getOutputValue();
                System.setProperty(resourceName, resourceValue);
            });
        });
    }
}
