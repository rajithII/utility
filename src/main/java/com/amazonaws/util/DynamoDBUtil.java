package com.amazonaws.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalOperator;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

public class DynamoDBUtil {

	private static final String amazonAWSRegion = "us-east-1";
	private static AmazonDynamoDB _dbClient;

	public AmazonDynamoDB getDBClient() {
		try {
			if (_dbClient == null) {
				_dbClient = AmazonDynamoDBClientBuilder.standard().withRegion(amazonAWSRegion).build();
			} else {
				_dbClient.listTables();
			}

		} catch (Exception ex) {
			_dbClient = AmazonDynamoDBClientBuilder.standard().withRegion(amazonAWSRegion).build();
		}
		return _dbClient;
	}

	public AWSCredentialsProvider getCredentialProvider(String amazonAWSAccessKey, String amazonAWSSecretKey) {
		AWSCredentialsProvider credentialsProvider = null;
		try {
			AWSCredentials credentials = new BasicAWSCredentials(amazonAWSAccessKey, amazonAWSSecretKey);
			credentialsProvider = new AWSStaticCredentialsProvider(credentials);
			credentialsProvider.getCredentials();
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
		return credentialsProvider;
	}

//	private void genereateScanRequest(String tableName, HashMap<String, List<Object>> conditionMap,
//			HashMap<String, String> columnsType, String conditionalOperator) {
//
//		ScanRequest scanRequest = new ScanRequest(tableName);
//		if()
//		
//		
//	}

	public JSONArray readItems(AmazonDynamoDB dbClient, String tableName,
			HashMap<String, List<AttributeValue>> conditionMap, HashMap<String, String> columnsType) throws Exception {
		JSONArray response = new JSONArray();
		ScanResult scanResult = null;
		try {

			ScanRequest scanRequest = new ScanRequest(tableName);
			scanRequest.setConditionalOperator(ConditionalOperator.OR);
			Condition condition = new Condition();
			for (String key : conditionMap.keySet()) {
				List<AttributeValue> attrValList = conditionMap.get(key);
				condition.setAttributeValueList(attrValList);
				condition.setComparisonOperator(ComparisonOperator.EQ);
				scanRequest.addScanFilterEntry(key, condition);
			}
			scanResult = dbClient.scan(scanRequest);
			for (Map<String, AttributeValue> item : scanResult.getItems()) {
				JSONObject itemJson = new JSONObject();
				for (String key : item.keySet()) {
					if (columnsType.containsKey(key)) {
						switch (columnsType.get(key).toLowerCase()) {
						case "ss":
							itemJson.put(key, item.get(key).getSS());
							break;
						case "bool":
							itemJson.put(key, item.get(key).getBOOL());
							break;
						case "long":
							itemJson.put(key, item.get(key).getN());
							break;
						case "s":
							itemJson.put(key, item.get(key).getS());
							break;
						default:
							break;
						}
					} else {
						itemJson.put(key, item.get(key).getS());
					}
				}
				response.put(itemJson);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error : " + e.getMessage());
			throw e;
		}
		return response;
	}

	public Boolean updateItemInDB(AmazonDynamoDB dbClient, String tableName, Map<String, AttributeValue> keyAttr,
			Map<String, AttributeValueUpdate> attributeUpdates) throws Exception {
		boolean result = false;
		try {
			UpdateItemRequest updateItemRequest = new UpdateItemRequest(tableName, keyAttr, attributeUpdates);
			UpdateItemResult response = dbClient.updateItem(updateItemRequest);
			result = true;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		return result;
	}

	public String retrieveItemFromDB(AmazonDynamoDB dbClient, String tableName, String keyColumnName,
			Object keyColumnValue) throws Exception {
		try {
			DynamoDB dynamoDB = new DynamoDB(dbClient);
			Table table = dynamoDB.getTable(tableName);
			Item item = table.getItem(keyColumnName, keyColumnValue);
			return item.toJSON();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

	public ItemCollection<ScanOutcome> scanDynamoDB(AmazonDynamoDB dbClient, String tableName) throws Exception {
		ItemCollection<ScanOutcome> items = null;
		try {

			DynamoDB dynamoDB = new DynamoDB(dbClient);
			Table table = dynamoDB.getTable(tableName);
			items = table.scan();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		return items;
	}

	public ItemCollection<ScanOutcome> scanDynamoDB(AmazonDynamoDB dbClient, String tableName, String keyColumnName,
			List<Object> values) throws Exception {
		ItemCollection<ScanOutcome> items = null;
		try {

			DynamoDB dynamoDB = new DynamoDB(dbClient);
			ScanFilter scanFilter = new ScanFilter(keyColumnName).in(values.toArray());
			Table table = dynamoDB.getTable(tableName);
			items = table.scan(scanFilter);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		return items;
	}

	public HashMap<String, Object> convert2HashMap(ItemCollection<ScanOutcome> items) {
		ArrayList<Object> rows = new ArrayList<>();
		HashMap<String, Object> response = new HashMap<>();
		HashMap<String, Object> status = new HashMap<>();
		try {

			Iterator<Item> iter = items.iterator();
			while (iter.hasNext()) {
				Item item = iter.next();
				Map<String, Object> itemMap = item.asMap();
				rows.add(itemMap);
			}
			status.put("code", 200);
			status.put("message", "SUCCESS");
			status.put("userMessage", "SUCCESS");
			response.put("status", status);
			response.put("data", rows);

		} catch (Exception ex) {
			String message = "Error parsing response. ";

			response = AppCoUtil.getErrorResponse(message + " Error : " + ex.getMessage(), message);
		}
		return response;
	}

	public void processDynDBRequest(HashMap<String, Object> parameters) {
		try {

			String action = parameters.get("action").toString();
			String tableName = parameters.get("tableName").toString();

			switch (action.toUpperCase()) {

			case "SCAN":
				String scanKeyColumnName = parameters.get("keyColumnName").toString();
				ArrayList<Object> scanKeyColumnValues = parameters.containsKey("values")
						? (ArrayList<Object>) parameters.get("values")
						: null;
				break;
			}

			String keyColumnName = parameters.get("keyColumnName").toString();
			ArrayList<Object> values = parameters.containsKey("values") ? (ArrayList<Object>) parameters.get("values")
					: null;

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
