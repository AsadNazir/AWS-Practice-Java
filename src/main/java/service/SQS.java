package service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.List;

public class SQS {

    public static void main(String[] args) {
        String queueUrl = "http://localhost:4566/000000000000/AsadQueue";

        // Initialize the SQS client
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                "http://localhost:4566", "eu-central-1"))
                .build();

        try {
            SQS s = new SQS();

            s.sendMsgToSqs("Hello from SQS", queueUrl,sqs);
            s.receiveMsg( queueUrl,sqs);

        } catch (AmazonClientException e) {
            e.printStackTrace();
        }
    }

    public void sendMsgToSqs(String msg, String queueUrl, AmazonSQS sqs) {

        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("Hello, World!");


        sqs.sendMessage(sendMessageRequest);
    }

    public void receiveMsg(String queueUrl, AmazonSQS sqs) {

        // Receive messages from the SQS queue
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(1);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        // Print received message(s)
        for (Message message : messages) {
            System.out.println("Received message: " + message.getBody());

            // Delete the received message from the queue
            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
        }

    }
}
