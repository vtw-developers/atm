package com.example.atm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HdfsProducerRoute  {

    public static void main(String[] args) throws Exception {

        System.out.println("Start");

        Class<?> aClass = Class.forName("org.apache.hive.jdbc.HiveDriver");
//        Connection cnct = DriverManager.getConnection("jdbc:hive2://192.168.0.89:9999/default", "hive", "hive");
        Connection cnct = DriverManager.getConnection("jdbc:hive2://192.168.0.83:10000/default", "hive", "hive");

        Statement stmt = cnct.createStatement();
        stmt.execute("insert into stock values ('20200105', 'SAM', 62000)");


//        ResultSet rs = stmt.executeQuery("select code_name, count(*), max(close_price), min(close_price) from stock group by code_name");
//        // Fetch each row from the result set
//        while (rs.next()) {
//            // Get the data from the row using the column index
//            String col1 = rs.getString(1);
//            String col2 = rs.getString(2);
//            String col3 = rs.getString(3);
//
//            System.out.println(col1 + ", " + col2 + ", " + col3);
//        }

        cnct.commit();

        cnct.close();

        System.out.println("End");
    }
}
