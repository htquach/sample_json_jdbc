/**
 * Copyright (c) 2016 Hong Quach
 * <p>
 * This source file is licensed under the "MIT License."  Please see the LICENSE
 * in this distribution for license terms.
 * <p>
 * This program includes functions to interact with live data feed and PostgreSQL
 */

package com.htquach.Sample_json_jdbc;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.w3c.dom.*;


import javax.xml.parsers.*;

import org.xml.sax.SAXException;

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, InterruptedException {
        // AccessPostgreSQL();
        // GetTrimetVehiclesPosition();

        int loopCount = 60 * 24;
        boolean twoMinutesReady = false;

        Connection conn = null;
        try {
            conn = GetDBConn();
            for (int w = 1; w <= loopCount; w++) {
                twoMinutesReady = !twoMinutesReady;
                if (twoMinutesReady) {
                    // Frequency of 2 minute enforced by TTIP data source
                    GetTtipTTDcuTraversals(conn);
                    GetTtipTTSegmentCalcs(conn);
                    GetTtipTTDcuInventory(conn);

                    // TODO:  implement these functions
                    // GetTtipTTSegInventory(conn); //every 24 hours
                }
                InsertVehiclesFeedToSQL(conn);
                System.out.println(String.format("%.3f%%", (w / ((double) loopCount) * 100)));
                System.out.println("Insert " + w + " of " + loopCount);
                // Frequency of 1 minute
                Thread.sleep(60000);
            }
        } catch (ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //Ignore Exception
                }
            }
        }
    }

    public static void AccessPostgreSQL() throws ClassNotFoundException, SQLException {
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

    private static void GetTtipTTDcuInventory(Connection conn) throws IOException, ParserConfigurationException, SAXException, SQLException, InterruptedException {
        String ttipID = System.getenv("TTIP_ID");
        if (ttipID == null || ttipID.isEmpty()) {
            throw new RuntimeException("TTIP Agency ID is required to query TTIP data.  " +
                    "Specify its value in the environment variable 'TTIP_ID'." +
                    "Detail http://www.tripcheck.com/ttipv2/Documents/TTIPSystemOverview.pdf");
        }
        String ttipDataRequestURLPrefix = "http://www.TripCheck.com/TTIPv2/TTIPData/DataRequest.aspx?uid=" + ttipID + "&fn=";

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new URL(ttipDataRequestURLPrefix + "TTDcuInventory").openStream());
        // Document doc = db.parse(new File("misc/TTIPData/TTDcuInventory.xml"));

        if (doc.getDocumentElement().getTextContent().contains("too soon to retrieve data")) {
            System.out.println(doc.getDocumentElement().getTextContent());
            return;
        }

        NodeList entries = doc.getElementsByTagName("Table");

        StringBuilder stmtBuilder = new StringBuilder();
        StringBuilder dcuIDs = new StringBuilder();

        stmtBuilder.append("INSERT INTO \"GTFS\".\"TTDcuInventory\" (\n" +
                "    \"DcuID\",\n" +
                "    \"DcuName\",\n" +
                "    \"Latitude\",\n" +
                "    \"Longitude\",\n" +
                "    \"Highway\",\n" +
                "    \"RoadwayNumber\",\n" +
                "    \"MilePoint\",\n" +
                "    \"LocationType\",\n" +
                "    \"IsActive\",\n" +
                "    \"OWNER\")" +
                "VALUES\n");
        for (int i = 0; i < entries.getLength(); i++) {
            Element current = (Element) entries.item(i);

            stmtBuilder.append("\t(");
            stmtBuilder.append(current.getElementsByTagName("DcuID").item(0).getTextContent());
            stmtBuilder.append(", '");
            stmtBuilder.append(current.getElementsByTagName("DcuName").item(0).getTextContent());
            stmtBuilder.append("', ");
            if (current.getElementsByTagName("Latitude").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("Latitude").item(0).getTextContent());
            } else {
                stmtBuilder.append("0.0");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("Longitude").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("Longitude").item(0).getTextContent());
            } else {
                stmtBuilder.append("0.0");
            }
            stmtBuilder.append(", '");
            if (current.getElementsByTagName("Highway").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("Highway").item(0).getTextContent());
            } else {
                stmtBuilder.append("Unknown");
            }
            stmtBuilder.append("', ");
            if (current.getElementsByTagName("RoadwayNumber").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("RoadwayNumber").item(0).getTextContent());
            } else {
                stmtBuilder.append("1");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("MilePoint").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("MilePoint").item(0).getTextContent());
            } else {
                stmtBuilder.append("0.0");
            }
            stmtBuilder.append(", '");
            if (current.getElementsByTagName("LocationType").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("LocationType").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append("', '");
            if (current.getElementsByTagName("IsActive").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("IsActive").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append("', '");
            if (current.getElementsByTagName("OWNER").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("OWNER").item(0).getTextContent());
            } else {
                stmtBuilder.append("Unknown");
            }


            dcuIDs.append(current.getElementsByTagName("DcuID").item(0).getTextContent());

            stmtBuilder.append("')");
            if (i < entries.getLength() - 1) {
                stmtBuilder.append(",\n");
                dcuIDs.append(",");
            }
        }
        stmtBuilder.append(";");

        if (entries.getLength() > 0) {
            Statement stmt = conn.createStatement();
            stmt.execute("DELETE FROM \"GTFS\".\"TTDcuInventory\" WHERE \"DcuID\" IN (" + dcuIDs.toString() + ");");
            stmt.execute(stmtBuilder.toString());
        }
        System.out.println(stmtBuilder.toString());
    }

    private static void GetTtipTTSegmentCalcs(Connection conn) throws IOException, ParserConfigurationException, SAXException, SQLException, InterruptedException {
        String ttipID = System.getenv("TTIP_ID");
        if (ttipID == null || ttipID.isEmpty()) {
            throw new RuntimeException("TTIP Agency ID is required to query TTIP data.  " +
                    "Specify its value in the environment variable 'TTIP_ID'." +
                    "Detail http://www.tripcheck.com/ttipv2/Documents/TTIPSystemOverview.pdf");
        }
        String ttipDataRequestURLPrefix = "http://www.TripCheck.com/TTIPv2/TTIPData/DataRequest.aspx?uid=" + ttipID + "&fn=";

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new URL(ttipDataRequestURLPrefix + "TTSegmentCalcs").openStream());
        // Document doc = db.parse(new File("C:\\Users\\htquach\\Desktop\\TTIP Data\\TTSegmentCalcs.xml"));

        if (doc.getDocumentElement().getTextContent().contains("too soon to retrieve data")) {
            System.out.println(doc.getDocumentElement().getTextContent());
            return;
        }

        NodeList entries = doc.getElementsByTagName("Table");

        StringBuilder stmtBuilder = new StringBuilder();
        stmtBuilder.append("INSERT INTO \"GTFS\".\"TTSegmentCalcs\"(\n" +
                "\t\"SegmentID\", \"SegmentCalcTime\", \"SegmentTravelTime\", \"CalcVariance\", \"CalcConfidenceInterval\", \"StdDeviationCalcSamplesRemoved\", \"StandardDeviationFitlerValue\", \"ExceededMaxFilter\", \"BelowMinFilter\")\n" +
                "\tVALUES\n");
        for (int i = 0; i < entries.getLength(); i++) {
            Element current = (Element) entries.item(i);

            stmtBuilder.append("\t(");
            stmtBuilder.append(current.getElementsByTagName("SegmentID").item(0).getTextContent());
            stmtBuilder.append(", ");
            stmtBuilder.append("TIMESTAMP WITH TIME ZONE '");
            stmtBuilder.append(current.getElementsByTagName("SegmentCalcTime").item(0).getTextContent());
            stmtBuilder.append("'");
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("SegmentTravelTime").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("SegmentTravelTime").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("CalcVariance").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("CalcVariance").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("CalcConfidenceInterval").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("CalcConfidenceInterval").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("StdDeviationCalcSamplesRemoved").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("StdDeviationCalcSamplesRemoved").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("StandardDeviationFitlerValue").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("StandardDeviationFitlerValue").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("ExceededMaxFilter").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("ExceededMaxFilter").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append(", ");
            if (current.getElementsByTagName("BelowMinFilter").item(0) != null) {
                stmtBuilder.append(current.getElementsByTagName("BelowMinFilter").item(0).getTextContent());
            } else {
                stmtBuilder.append("0");
            }
            stmtBuilder.append(")");
            if (i < entries.getLength() - 1) {
                stmtBuilder.append(",\n");
            }
        }


//        <SegmentID>2381</SegmentID>
//        <SegmentCalcTime>2016-10-28T17:30:20.677-07:00</SegmentCalcTime>
//        <StdDeviationCalcSamplesRemoved>0</StdDeviationCalcSamplesRemoved>
//        <ExceededMaxFilter>0</ExceededMaxFilter>
//        <BelowMinFilter>57</BelowMinFilter>

        if (entries.getLength() > 0) {
            Statement stmt = conn.createStatement();
            stmt.execute(stmtBuilder.toString());
        }
        System.out.println(stmtBuilder.toString());
    }

    private static void GetTtipTTDcuTraversals(Connection conn) throws IOException, ParserConfigurationException, SAXException, SQLException, InterruptedException {
        String ttipID = System.getenv("TTIP_ID");
        if (ttipID == null || ttipID.isEmpty()) {
            throw new RuntimeException("TTIP Agency ID is required to query TTIP data.  " +
                    "Specify its value in the environment variable 'TTIP_ID'." +
                    "Detail http://www.tripcheck.com/ttipv2/Documents/TTIPSystemOverview.pdf");
        }
        String ttipDataRequestURLPrefix = "http://www.TripCheck.com/TTIPv2/TTIPData/DataRequest.aspx?uid=" + ttipID + "&fn=";

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new URL(ttipDataRequestURLPrefix + "TTDcuTraversals").openStream());
        //Document doc = db.parse(new File("PathToXMLFileForDebugTTDcuTraversals.xml"));

        if (doc.getDocumentElement().getTextContent().contains("too soon to retrieve data")) {
            System.out.println(doc.getDocumentElement().getTextContent());
            return;
        }

        NodeList entries = doc.getElementsByTagName("Table");

        StringBuilder stmtBuilder = new StringBuilder();
        stmtBuilder.append("INSERT INTO \"GTFS\".\"TTDcuTraversals\"(\n" +
                "\t\"TraversalID\", \"SegmentID\", \"TraversalEndDateTime\", \"TraversalTravelDateTime\", \"DataSourceName\", \"TraversalSubmittedDateTime\")\n" +
                "\tVALUES\n");
        for (int i = 0; i < entries.getLength(); i++) {
            Element current = (Element) entries.item(i);

            stmtBuilder.append("\t(");
            stmtBuilder.append(current.getElementsByTagName("TraversalID").item(0).getTextContent());
            stmtBuilder.append(", ");
            stmtBuilder.append(current.getElementsByTagName("SegmentID").item(0).getTextContent());
            stmtBuilder.append(", ");
            stmtBuilder.append("TIMESTAMP WITH TIME ZONE '");
            stmtBuilder.append(current.getElementsByTagName("TraversalEndDateTime").item(0).getTextContent());
            stmtBuilder.append("'");
            stmtBuilder.append(", ");
            stmtBuilder.append(current.getElementsByTagName("TraversalTravelDateTime").item(0).getTextContent());
            stmtBuilder.append(", ");
            stmtBuilder.append("'");
            stmtBuilder.append(current.getElementsByTagName("DataSourceName").item(0).getTextContent());
            stmtBuilder.append("'");
            stmtBuilder.append(", ");
            stmtBuilder.append("TIMESTAMP WITH TIME ZONE '");
            stmtBuilder.append(current.getElementsByTagName("TravesalSubmittedDateTime").item(0).getTextContent());
            stmtBuilder.append("'");
            stmtBuilder.append(")");
            if (i < entries.getLength() - 1) {
                stmtBuilder.append(",\n");
            }
        }

        if (entries.getLength() > 0) {
            Statement stmt = conn.createStatement();
            stmt.execute(stmtBuilder.toString());
        }
        System.out.println(stmtBuilder.toString());
    }

    public static void GetTrimetVehiclesPosition() throws IOException {
        JSONObject jsonObject = GetVehiclesFeed();
        JSONArray vehicles = jsonObject.getJSONObject("resultSet").getJSONArray("vehicle");
        for (int i = 0; i < vehicles.length(); i++) {
            System.out.println(i + "   \t>>> " + vehicles.getJSONObject(i).optString("signMessage"));
        }
    }

    public static void InsertVehiclesFeedToSQL(Connection conn) throws ClassNotFoundException, IOException, InterruptedException {
        try {
            Statement stmt = conn.createStatement();
            ResultSet colTypesResult = stmt.executeQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'vehicles_log';");

            HashMap<String, String> colTypes = new HashMap<String, String>();
            while (colTypesResult.next()) {
                colTypes.put(colTypesResult.getString(1), colTypesResult.getString(2));
            }

            JSONObject jsonObject = GetVehiclesFeed();
            Long queryTime = jsonObject.getJSONObject("resultSet").getLong("queryTime");
            JSONArray vehicles = jsonObject.getJSONObject("resultSet").getJSONArray("vehicle");

            Iterator<String> keysIter = vehicles.getJSONObject(0).keys();
            ArrayList<String> keys = new ArrayList<String>();
            while (keysIter.hasNext()) {
                keys.add(keysIter.next());
            }

            StringBuilder insertStmt = new StringBuilder();
            insertStmt.append("INSERT INTO \"GTFS\".vehicles_log\n");
            insertStmt.append("\t(");
            insertStmt.append("\"queryTime\", ");
            int keyCount = keys.size();
            for (int i = 0; i < keyCount; i++) {
                insertStmt.append("\"");
                insertStmt.append(keys.get(i));
                insertStmt.append("\"");
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
                insertStmt.append(queryTime);
                insertStmt.append(", ");
                for (int n = 0; n < keyCount; n++) {
                    String key = keys.get(n);
                    String s = colTypes.get(key);
                    if (s.equals("integer")) {
                        insertStmt.append(vehicle.optInt(key));

                    } else if (s.equals("boolean")) {
                        insertStmt.append(vehicle.optBoolean(key));

                    } else if (s.equals("real")) {
                        insertStmt.append(vehicle.optDouble(key));

                    } else if (s.equals("bigint")) {
                        insertStmt.append(vehicle.optBigInteger(key, null));

                    } else {
                        insertStmt.append("'");
                        insertStmt.append(vehicle.optString(key));
                        insertStmt.append("'");
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

            if (vehicleCount > 0) {
                stmt.execute(insertStmt.toString());
                System.out.println(insertStmt.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Connection GetDBConn() throws ClassNotFoundException, SQLException {
        String db_host = System.getenv("DB_HOST");
        if (db_host == null || db_host.isEmpty()) {
            throw new RuntimeException("DB host has not been specified.  Specify its value in the environment variable 'DB_HOST'.");
        }
        String db_name = System.getenv("DB_NAME");
        if (db_name == null || db_name.isEmpty()) {
            db_name = "GTFS";
        }
        String db_user = System.getenv("DB_USER");
        if (db_user == null || db_user.isEmpty()) {
            throw new RuntimeException("DB user has not been specified.  Specify its value in the environment variable 'DB_USER'.");
        }
        String db_pw = System.getenv("DB_PW");
        if (db_pw == null) {
            throw new RuntimeException("DB Password cannot be null.  Specify its value in the environment variable 'DB_PW'.");
        }
        String db_schema = System.getenv("DB_SCHEMA");
        if (db_schema == null || db_schema.isEmpty()) {
            db_schema = "public";
        }

        String dbURL = "jdbc:postgresql://" + db_host + "/" + db_name + "?user=" + db_user + "&password=" + db_pw + "&currentSchema=" + db_schema;
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(dbURL);
    }

    private static JSONObject GetVehiclesFeed() throws IOException {
        String appID = System.getenv("TRIMETAPPID");
        if (appID == null || appID.isEmpty()) {
            throw new RuntimeException("TriMet AppID is required to query realtime data.  " +
                    "Specify its value in the environment variable 'TRIMETAPPID");
        }
        URL serviceURL = new URL("http://developer.trimet.org/ws/v2/vehicles/AppID/" + appID);
        InputStream inputStream = serviceURL.openStream();
        JSONTokener jsonTokener = new JSONTokener(inputStream);
        return new JSONObject(jsonTokener);
    }
}
