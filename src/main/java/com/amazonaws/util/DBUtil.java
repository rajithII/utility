package com.amazonaws.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.amazonaws.services.lambda.runtime.Context;

public class DBUtil {

	private static HashMap<String, Connection> connections;
	static {
		connections = new HashMap<>();
	}
	
	private Connection getConnection(String alias, Context context) {
		Connection conn = null;

		try {
			if (connections.containsKey(alias)) {
				Connection connObj = connections.get(alias);

				if (connObj != null && !connObj.isClosed()) {
					Statement stmt = null;
					ResultSet rs = null;
					Boolean connExist = true;
					try {
						String query = "Select * from CCTConfig";
						stmt = connObj.createStatement();
						rs = executeQueryInDB(connObj, query);
					} catch (Exception ex) {
						connExist = false;
					} finally {
						DBUtil.closeConnection(rs, stmt, (connExist ? null : connObj));
					}

					if (!connExist) {
						connections.put(alias, getNewConnection(alias));
						context.getLogger()
								.log("Connection is not existing. Closing old connection, opening new connection");
					}
				} else {
					connections.put(alias, getNewConnection(alias));
					context.getLogger().log("Connection is closed. Opening new connection");
				}
			} else {
				connections.put(alias, getNewConnection(alias));
				context.getLogger().log("Opneing new Connection");
			}

			conn = connections.get(alias);

		} catch (Exception ex) {
			conn = getNewConnection(alias);
		}

		return conn;
	}

	public Connection getNewConnection(String alias) {
		Connection conn = null;
		try {
			HashMap<String, String> connMap = AppCoUtil.getConnStringFromConfig(alias);
			conn = DriverManager.getConnection(connMap.get(AppCoUtil.CONNECTIONSTRING),
					connMap.get(AppCoUtil.ENDPOINTUSERNAME), connMap.get(AppCoUtil.ENDPOINTPASSWORD));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	private HashMap<String, Object> convertResult2HashMap(ResultSet rs) {
		HashMap<String, Object> result = new HashMap<>();
		HashMap<String, Object> status = new HashMap<>();
		try {
			ArrayList<Object> rows = new ArrayList<>();
			ResultSetMetaData md = rs.getMetaData();
			int columns = md.getColumnCount();

			while (rs.next()) {
				HashMap row = new HashMap(columns);
				for (int i = 1; i <= columns; ++i) {
					row.put(md.getColumnName(i), rs.getObject(i));
				}
				rows.add(row);
			}
			result.put("rows", rows);
			status.put("code", 200);
			status.put("message", "Results loaded successfully");
			status.put("userMessage", "Results loaded successfully");
			result.put("status", status);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			status.put("code", 400);
			status.put("message", "Results loading failed. Error = " + ex.getMessage());
			status.put("userMessage", "Results loading failed.");
			result.put("status", status);
		}
		return result;
	}

	private ResultSet executeQueryInDB(Connection conn, String query) throws Exception {
		ResultSet rs = null;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeQuery(query);
			rs = stmt.getResultSet();			
		} catch (Exception e) {
			throw e;
		} 
		return rs;
	}

	public HashMap<String, Object> executeQuery(Connection conn, String query) {
		ResultSet rs = null;
		HashMap<String, Object> result = new HashMap<>();
		try {
			rs = executeQueryInDB(conn, query);			
			result = convertResult2HashMap(rs);
			
		} catch (Exception e) {
			HashMap<String, Object> status = new HashMap<>();
			e.printStackTrace();
			status.put("code", 400);
			status.put("message", "Results loading failed. Error = " + e.getMessage());
			status.put("userMessage", "Results loading failed.");
			result.put("status", status);
		} finally {
			closeConnection(rs, null, null);
		}
		return result;
	}
	
	public HashMap<String, Object> executeUpdate(Connection conn, String query) {
		Statement stmt = null;
		HashMap<String, Object> response = new HashMap<>();
		HashMap<String, Object> status = new HashMap<>();
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
			response.put("success", true);
			status.put("code", 200);
			status.put("message", "Update successful.");
			status.put("userMessage", "Updates successful.");
			response.put("status", status);
			
		} catch (Exception e) {
			response.put("success", false);
			e.printStackTrace();
			status.put("code", 400);
			status.put("message", "Update failed. Error = " + e.getMessage());
			status.put("userMessage", "Updates failed.");
			response.put("status", status);
		}
		return response;
	}

	public static void closeConnection(ResultSet rs, Statement stmt, Connection conn) {
		try {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (Exception e) {
				System.out.println("unable to close ResultSet");
			}
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				System.out.println("unable to close Statement");
			}
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				System.out.println("unable to close Connection");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
