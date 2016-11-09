/**
 * Copyright (c) 2015 Hong Quach
 *
 * This source file is licensed under the "MIT License."  Please see the LICENSE
 * in this distribution for license terms.
 */

/** This program includes functions to interact with live data feed and PostgreSQL
 *   1) A method to execute query GTFS data against PostgreSQL;
 *   2) A method to retrieve TriMet realtime data through TriMet Webservices
 *   3) A method to insert data into PostgreSQL
 */

package com.htquach.Sample_json_jdbc;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        //AccessPostgreSQL();
        //GetTrimetVehiclesPosition();

        InsertVehiclesFeedToSQL();
    }

    private static void InsertVehiclesFeedToSQL() throws ClassNotFoundException, SQLException, IOException {
        Connection conn = GetDBConn();
        Statement stmt = conn.createStatement();
        ResultSet colTypesResult = stmt.executeQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'vehicles_log';");

        HashMap<String, String> colTypes = new HashMap<>();
        while (colTypesResult.next()) {
            colTypes.put(colTypesResult.getString(1), colTypesResult.getString(2));
        }

        JSONObject jsonObject = GetVehiclesFeed();
        Long queryTime = jsonObject.getJSONObject("resultSet").getLong("queryTime");
        JSONArray vehicles = jsonObject.getJSONObject("resultSet").getJSONArray("vehicle");

        Iterator<String> keysIter = vehicles.getJSONObject(0).keys();
        ArrayList<String> keys = new ArrayList<>();
        while (keysIter.hasNext()) {
            keys.add(keysIter.next());
        }

        StringBuilder insertStmt = new StringBuilder();
        insertStmt.append("INSERT INTO \"GTFS\".vehicles_log\n");
        insertStmt.append("\t(");
        insertStmt.append("\"queryTime\", ");
        int keyCount = keys.size();
        for (int i = 0; i < keyCount; i++) {
            insertStmt.append("\"" + keys.get(i) + "\"");
            if (i < keyCount - 1) {
                insertStmt.append(", ");
            }
        }
        insertStmt.append(")\n");
        insertStmt.append("VALUES\n");

        int vehicleCount = vehicles.length();
        for (int m = 0; m < vehicleCount; m++) {
            JSONObject vehicle = vehicles.getJSONObject(m);
            insertStmt.append("\t(");
            insertStmt.append(queryTime + ", ");
            for (int n = 0; n < keyCount; n++) {
                String key = keys.get(n);
                switch (colTypes.get(key)) {
                    case "integer":
                        insertStmt.append(vehicle.optInt(key));
                        break;
                    case "boolean":
                        insertStmt.append(vehicle.optBoolean(key));
                        break;
                    case "real":
                        insertStmt.append(vehicle.optDouble(key));
                        break;
                    case "bigint":
                        insertStmt.append(vehicle.optBigInteger(key, null));
                        break;
                    default:
                        insertStmt.append("'" + vehicle.optString(key) + "'");
                }
                if (n < keyCount - 1) {
                    insertStmt.append(", ");
                }
            }

            insertStmt.append(")");
            if (m < vehicleCount - 1) {
                insertStmt.append(",\n");
            }
        }
        insertStmt.append(";");
        stmt.execute(insertStmt.toString());
    }

    private static void AccessPostgreSQL() throws ClassNotFoundException, SQLException {
        Connection conn = GetDBConn();
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("SELECT DISTINCT stop_name FROM \"GTFS\".stops;");
        System.out.println("Column 1 of data returned from database");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
        resultSet.close();
        stmt.close();
    }

    private static Connection GetDBConn() throws ClassNotFoundException, SQLException {
        String db_host = System.getenv("DB_HOST");
        if (db_host == null || db_host.isEmpty()) {
            db_host = "localhost";
        }
        String db_name = System.getenv("DB_NAME");
        if (db_name == null || db_name.isEmpty()) {
            db_name = "GTFS";
        }
        String db_user = System.getenv("DB_USER");
        if (db_user == null || db_user.isEmpty()) {
            throw new RuntimeException("DB user is not specified.  Specify its value in the environment variable 'DB_USER'.");
        }
        String db_pw = System.getenv("DB_PW");
        if (db_pw == null) {
            throw new RuntimeException("DB Password cannot be null.  Specify its value in the environment variable 'DB_PW'.");
        }
        String db_schema = System.getenv("DB_SCHEMA");
        if (db_schema == null || db_schema.isEmpty()) {
            db_schema = "public";
        }

        String dbURL = "jdbc:postgresql://"+db_host+"/"+db_name+"?user="+db_user+"&password="+db_pw+"&currentSchema="+db_schema;
        Connection conn = null;
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(dbURL);
        return conn;
    }

    private static void GetTrimetVehiclesPosition() throws IOException {
        JSONObject jsonObject = GetVehiclesFeed();
        JSONArray vehicles = jsonObject.getJSONObject("resultSet").getJSONArray("vehicle");
        for (int i = 0; i < vehicles.length(); i++) {
            System.out.println(i+ "   \t>>> " + vehicles.getJSONObject(i).optString("signMessage"));
        }
    }

    private static JSONObject GetVehiclesFeed() throws IOException {
        String appID = System.getenv("TRIMETAPPID");
        if (appID == null || appID.isEmpty()) {
            throw new RuntimeException("TriMet AppID is required to query realtime data.  " +
                    "Specify its value in the environment variable 'TRIMETAPPID");
        }
        URL serviceURL = new URL("http://developer.trimet.org/ws/v2/vehicles/AppID/"+appID);
        InputStream inputStream = serviceURL.openStream();
        JSONTokener jsonTokener = new JSONTokener(inputStream);
        return new JSONObject(jsonTokener);
    }
}
