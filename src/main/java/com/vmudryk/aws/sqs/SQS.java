package com.vmudryk.aws.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Volodymyr on 10.10.2015.
 */
public class SQS {

    private static final String QUEUE_URL = "https://......./DEV_ANDROIDS_EVERYWHERE";
    private AmazonSQSClient sqsClient;

    public SQS(AWSCredentials awsCredentials) {
        sqsClient = new AmazonSQSClient(awsCredentials);
    }

    public void sendMessage(String message) {
        sqsClient.sendMessage(QUEUE_URL, message);
    }

    public void sendMessages(List<String> messages) {
        List<SendMessageBatchRequestEntry> sendMessageBatchRequestEntries = new ArrayList<>();
        messages.forEach(message -> {
            SendMessageBatchRequestEntry sendMessageBatchRequestEntry = new SendMessageBatchRequestEntry();
            sendMessageBatchRequestEntry.setMessageBody(message);
            sendMessageBatchRequestEntries.add(sendMessageBatchRequestEntry);
        });
        SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(QUEUE_URL, sendMessageBatchRequestEntries);
        sqsClient.sendMessageBatch(sendMessageBatchRequest);
    }

    public Message receiveMessage() {
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(QUEUE_URL);
        List<Message> messages = receiveMessageResult.getMessages();

        return messages.iterator().next();
    }

    public List<Message> receiveMessages(int maxNumberOfMessages) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QUEUE_URL);
        receiveMessageRequest.setMaxNumberOfMessages(maxNumberOfMessages);

        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResult.getMessages();

        return messages;
    }

    public void deleteMessage(Message message) {
        sqsClient.deleteMessage(QUEUE_URL, message.getReceiptHandle());
    }

    public void deleteMessages(List<Message> messages) {
        List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = new ArrayList<>();
        messages.forEach(message -> {
            DeleteMessageBatchRequestEntry deleteMessageBatchRequestEntry = new DeleteMessageBatchRequestEntry();
            deleteMessageBatchRequestEntry.setReceiptHandle(message.getReceiptHandle());
            deleteMessageBatchRequestEntries.add(deleteMessageBatchRequestEntry);
        });

        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(QUEUE_URL, deleteMessageBatchRequestEntries);
        sqsClient.deleteMessageBatch(deleteMessageBatchRequest);
    }
}
