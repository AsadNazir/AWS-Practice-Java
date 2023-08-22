package service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DynamoDBExample {
    public static void main(String[] args) {

        String region = "eu-central-1"; // Replace with your desired region

        AmazonDynamoDB client = AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:4566", region))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);


//     //  Creating a table first
//         createTable("NewBooks2", dynamoDB);
//        System.out.println("Table is Up and Active : ");


        String tableName = "NewBooks";
        Table table = dynamoDB.getTable(tableName);
        String json ="{\"name\": \"John Doe\"}";
        Item bookItem = new Item()
                .withPrimaryKey("ID", 90).withString("Nomenclature", "My Name")
                .withJSON("data",json)
                .withString("hahah","Dora");
        table.putItem(bookItem);


        Map<String, AttributeValue> lastKeyEvaluated = null;
        do {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName("NewBooks")
                    .withLimit(10)
                    .withExclusiveStartKey(lastKeyEvaluated);

            ScanResult result = client.scan(scanRequest);
            for (Map<String, AttributeValue> item : result.getItems()){
                printItem(item);
            }
            lastKeyEvaluated = result.getLastEvaluatedKey();
        } while (lastKeyEvaluated != null);


    }


    public static void printItem(Map<String, AttributeValue> item) {
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            String key = entry.getKey();
            AttributeValue value = entry.getValue();

            if(key.equalsIgnoreCase("data"))
            {
                System.out.println(key+ ": "+ value.toString());
            }
            // Check the attribute type
            if (value.getS() != null) {
                System.out.println(key + ": " + value.getS());
            } else if (value.getN() != null) {
                System.out.println(key + ": " + value.getN());
            } else if (value.getB() != null) {
                // Handle binary data if needed
                System.out.println(key + ": [Binary Data]");
            }
        }
    }


    public static void createTable(String tableName, DynamoDB client) {
        try {
            System.out.println("Creating the table, wait...");
            Table table = client.createTable(tableName,
                    Arrays.asList(
                            new KeySchemaElement("ID", KeyType.HASH), // the partition key
                            // the sort key
                            new KeySchemaElement("Nomenclature", KeyType.RANGE)
                    ),
                    Arrays.asList(
                            new AttributeDefinition("ID", ScalarAttributeType.N),
                            new AttributeDefinition("Nomenclature", ScalarAttributeType.S)
                    ),
                    new ProvisionedThroughput(10L, 10L)
            );
            table.waitForActive();
            System.out.println("Table created successfully.  Status: " +
                    table.getDescription().getTableName());

        } catch (Exception e) {
            System.err.println("Cannot create the table: ");
            System.err.println(e.getMessage());
        }
    }

}
