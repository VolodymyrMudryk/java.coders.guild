package com.vmudryk.aws.dynamo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Volodymyr on 07.10.2015.
 */
public class Dynamo {

    private static final String TABLE_NAME = "Development3";
    private static final String UUID_KEY = "UUID";
    private static final String TIME_STAMP_KEY = "TimeStamp";
    private AmazonDynamoDBClient amazonDynamoDBClient;

    public Dynamo(AWSCredentials awsCredentials) {
        amazonDynamoDBClient = getAmazonDynamoDBClient(awsCredentials);
    }

    public List<String> getTablesList() {
        ListTablesResult listTablesResult = amazonDynamoDBClient.listTables();
        return listTablesResult.getTableNames();
    }

    public List<String> getTablesList(int limit) {
        ListTablesResult listTablesResult = amazonDynamoDBClient.listTables(limit);
        return listTablesResult.getTableNames();
    }

    public List<String> getTablesList(String exclusiveStartTableName, int limit) {
        ListTablesResult listTablesResult = amazonDynamoDBClient.listTables(exclusiveStartTableName, limit);
        return listTablesResult.getTableNames();
    }

    public void updateItem(String uuid, String timestamp, Map<String, String> values) {
        Map<String, AttributeValue> itemAttributes = getItemAttributes(uuid, timestamp);

        Map<String, AttributeValueUpdate> itemAttributeUpdates = new HashMap<>();
        values.entrySet().forEach(entry ->
                itemAttributeUpdates.put(entry.getKey(), new AttributeValueUpdate().withValue(new AttributeValue().withN(entry.getValue()))));

        amazonDynamoDBClient.updateItem(TABLE_NAME, itemAttributes, itemAttributeUpdates);
    }

    public void putItem(String uuid, String timestamp, Map<String, String> values) {
        Map<String, AttributeValue> itemAttributes = getItemAttributes(uuid, timestamp);

        values.entrySet().forEach(entry ->
                itemAttributes.put(entry.getKey(), new AttributeValue().withN(entry.getValue())));

        amazonDynamoDBClient.putItem(TABLE_NAME, itemAttributes);
    }

    public Map<String, AttributeValue> getItem(String uuid, String timestamp) {
        Map<String, AttributeValue> itemAttributes = getItemAttributes(uuid, timestamp);

        GetItemResult getItemResult = amazonDynamoDBClient.getItem(TABLE_NAME, itemAttributes);

        Map<String, AttributeValue> items = getItemResult.getItem();

        return items;
    }

    public List<Map<String, AttributeValue>> query(String uuid, String timestamp) {
        Map<String, Condition> keyConditions = new HashMap<>();

        Condition hashKeyCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(uuid));
        keyConditions.put(UUID_KEY, hashKeyCondition);

        Condition rangeCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(timestamp));
        keyConditions.put(TIME_STAMP_KEY, rangeCondition);

        QueryRequest queryRequest = new QueryRequest()
                .withTableName(TABLE_NAME)
                .withKeyConditions(keyConditions);

        QueryResult queryResult = amazonDynamoDBClient.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResult.getItems();

        return items;
    }

    public void deleteItem(String uuid, String timestamp) {
        Map<String, AttributeValue> itemAttributes = getItemAttributes(uuid, timestamp);

        amazonDynamoDBClient.deleteItem(TABLE_NAME, itemAttributes);
    }

    private AmazonDynamoDBClient getAmazonDynamoDBClient(AWSCredentials awsCredentials) {
        return new AmazonDynamoDBClient(awsCredentials);
    }

    private Map<String, AttributeValue> getItemAttributes(String uuid, String timestamp) {
        Map<String, AttributeValue> itemAttributes = new HashMap<>();
        itemAttributes.put(UUID_KEY, new AttributeValue(uuid));
        itemAttributes.put(TIME_STAMP_KEY, new AttributeValue().withN(timestamp));
        return itemAttributes;
    }
}
