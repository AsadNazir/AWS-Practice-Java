package service;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.*;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.List;

public class SNS {
    public static void main(String[] args) {

        String localstackEndpoint = "http://localhost:4566";

        AmazonSNS snsClient = AmazonSNSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(localstackEndpoint, "eu-central-1"))
                .build();

        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                "http://localhost:4566", "eu-central-1"))
                .build();

        String topicArn = createSNSTopic(snsClient, "MyTopic");

        String queueArn = "arn:aws:sqs:eu-central-1:000000000000:AsadQueue";

        String subscriptionArn = subscribeQueueToTopic(snsClient, topicArn, queueArn);
        String queueUrl = "http://localhost:4566/000000000000/AsadQueue";

        //Creating SQS Client


        // Publish a message to the SNS topic
        String message = "Hello, SNS!";
        PublishRequest publishRequest = new PublishRequest(topicArn, message);
        snsClient.publish(publishRequest);

        // Check if the SQS queue received the message
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(10);
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();


        System.out.println("Topic ARN: " + topicArn);
        System.out.println("Subscription ARN: " + subscriptionArn);

        System.out.println("Received messages in SQS queue:");
        for (Message sqsMessage : messages) {
            System.out.println("Message: " + sqsMessage.getBody());
        }


    }

    private static String createSNSTopic(AmazonSNS snsClient, String topicName) {
        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
        CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);
        return createTopicResult.getTopicArn();
    }

    private static String subscribeQueueToTopic(AmazonSNS snsClient, String topicArn, String queueArn) {
        SubscribeRequest subscribeRequest = new SubscribeRequest(topicArn, "sqs", queueArn);
        SubscribeResult subscribeResult = snsClient.subscribe(subscribeRequest);
        return subscribeResult.getSubscriptionArn();
    }
}
