/**
 * Copyright (c) 2015 Hong Quach
 *
 * This source file is licensed under the "MIT License."  Please see the LICENSE
 * in this distribution for license terms.
 */

/** This program includes two key methods:
 *   1) A method to execute query GTFS data against PostgreSQL;
 *   2) A method to retrieve TriMet realtime data through TriMet Webservices
 */

package com.htquach.Sample_json_jdbc;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, MalformedURLException {
        System.out.println("execute query against postgreSQL");
        AccessPostgreSQL();

        System.out.println("\n\n=============================================\n\n");
        System.out.println("Vehicles' sign message from TriMet realtime");
        GetTrimetVehiclesPosition();
    }

    private static void AccessPostgreSQL() throws ClassNotFoundException, SQLException {
        String db_user = System.getenv("DB_USER");
        if (db_user == null || db_user.isEmpty()) {
            System.out.println("DB user is not specified.  Specify its value in the environment variable 'DB_USER'.");
            return;
        }
        String db_pw = System.getenv("DB_PW");
        if (db_pw == null) {
            System.out.println("DB Password cannot be null.  Specify its value in the environment variable 'DB_PW'.");
            return;
        }

        String dbURL = "jdbc:postgresql://localhost/GTFS?user="+db_user+"&password="+db_pw;
        Connection conn = null;
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(dbURL);
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("SELECT DISTINCT stop_name FROM stops;");
        System.out.println("Column 1 of data returned from database");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
        resultSet.close();
        stmt.close();
    }

    private static void GetTrimetVehiclesPosition() throws MalformedURLException {
        String appID = System.getenv("TriMetAppID");
        if (appID == null || appID.isEmpty()) {
            System.out.println("TriMet AppID is required to query realtime data.  " +
                    "Specify its value in the environment variable 'TrimMetAppID");
        }
        URL serviceURL = new URL("http://developer.trimet.org/ws/v2/vehicles/AppID/"+appID);
        try {
            InputStream inputStream = serviceURL.openStream();
            JSONTokener jsonTokener = new JSONTokener(inputStream);
            JSONObject jsonObject = new JSONObject(jsonTokener);
            JSONArray vehicles = jsonObject.getJSONObject("resultSet").getJSONArray("vehicle");
            for (int i = 0; i < vehicles.length(); i++) {
                System.out.println(vehicles.getJSONObject(i).getString("signMessage"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
