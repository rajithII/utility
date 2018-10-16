package com.amazonaws.util;

import java.util.HashMap;

import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.lambda.runtime.Context;

public class AppCoUtil {
	
	public static final String ENDPOINT = "ENDPOINT";
	public static final String ENDPOINTDBNAME = "ENDPOINTDBNAME";
	public static final String CONNECTIONSTRING = "CONNECTIONSTRING";
	public static final String ENDPOINTUSERNAME = "ENDPOINTUSERNAME";
	public static final String ENDPOINTPASSWORD = "ENDPOINTPASSWORD";
	
	public static String chummaFunction() {
		return "veruthe";
	}
	
	public static JSONObject getConfigFromDynamoDB(String alias) {
		JSONObject config = null;		
		String configTableKeyColumn = "alias";
		String configTableName = "Config";
		try {
			DynamoDBUtil dynDBUtil = new DynamoDBUtil();
			AmazonDynamoDB dbClient = dynDBUtil.getDBClient();
			String configStr = dynDBUtil.retrieveItemFromDB(dbClient, configTableName, configTableKeyColumn, alias);
			config = new JSONObject(configStr);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		return config;
	}
	
	public static HashMap<String, Object> getErrorResponse(String message, String userMessage){
		
		HashMap<String, Object> status = new HashMap<>();
		status.put("code", 400);
		status.put("message", message);
		status.put("userMessage", userMessage);
		HashMap<String, Object> retJson = new HashMap<>();
		retJson.put("status", status);
		return retJson;
	}
	
	public static HashMap<String, String> getConnStringFromConfig(String alias) {
		HashMap<String, String> connMap = new HashMap<String, String>();
		String endPoint="iihccdb.ccotz7ldulel.us-west-2.rds.amazonaws.com:3306";
		String endPointUserName="master";
		String endPointPassword="cctiia1234";
		String endPointDBName = "gunbroker_staging";
		try {
			JSONObject row = getConfigFromDynamoDB(alias);
			JSONObject configJSON = (row != null && row.has("config")) ? new JSONObject(row.getString("config")) : null;
			if(configJSON != null &&  configJSON.has("DB")) {
				JSONObject dbConfig = configJSON.getJSONObject("DB");
				endPoint = dbConfig.has(ENDPOINT) ? dbConfig.getString(ENDPOINT) : endPoint;
				endPointUserName = dbConfig.has(ENDPOINTUSERNAME) ? dbConfig.getString(ENDPOINTUSERNAME) : endPointUserName;
				endPointPassword = dbConfig.has(ENDPOINTPASSWORD) ? dbConfig.getString(ENDPOINTPASSWORD) : endPointPassword;
				endPointDBName = dbConfig.has(ENDPOINTDBNAME) ? dbConfig.getString(ENDPOINTDBNAME) : endPointDBName;
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		String connStr = "jdbc:mysql://" + endPoint + "/" + endPointDBName;
		
		connMap.put(AppCoUtil.CONNECTIONSTRING, connStr);
		connMap.put(AppCoUtil.ENDPOINTUSERNAME, endPointUserName);
		connMap.put(AppCoUtil.ENDPOINTPASSWORD, endPointPassword);
		
		return connMap;
	}
	
	public static String getAliasFromContext(Context context) {
		String alias = "";
		try {
			String functionName = context.getFunctionName();
			alias = context.getInvokedFunctionArn().split(":")[context.getInvokedFunctionArn().split(":").length-1];
			if (alias.equals(functionName)) {
				alias = "staging";
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
			alias = "staging";
		}
		return alias;
	}

	public static String getDynamoDBTableName(String tableName, Context context) {
		String response = tableName;
		String alias = getAliasFromContext(context);
		if (!alias.equals("prod")) {
			response = tableName + "_" + alias;
		}
		context.getLogger().log("Table = " + tableName);
		return response;
	}

}
