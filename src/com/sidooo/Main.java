package com.sidooo;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.*;

public class Main {

    private static void exportData(ResultSet rs, PrintWriter output) {
        try {
            long  count=0;
            while(rs.next()) {

                count++;
                if (count % 5000 == 0) {
                    System.out.println("Export "+count+" records.");
                }

                String  name = rs.getString("truename");
                String  account = rs.getString("username");
                String  sex = rs.getString("sex");
                String  age = ""+rs.getInt("age");
                String  password = rs.getString("password");
                String  salt = rs.getString("salt");
                String  email=rs.getString("email");
                String  qq = rs.getString("qq");
                String  sfz = rs.getString("sfz");
                String  mobile = rs.getString("mobile");
                String  address = rs.getString("address");
                String  source = rs.getString("comefrom");


                String sep = ",";
                String line = name +sep
                        +account+sep
                        +sex+sep
                        +age+sep
                        +password+sep
                        +salt+sep
                        +email+sep
                        +qq+sep
                        +sfz+sep
                        +mobile+sep
                        +address+sep
                        +source;
                output.println(line);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void usage() {

        System.out.println("sgk_export <database> <table>.");
    }

    public static void main(String[] args) {
	    // write your code here

        String database = args[0];
        String table = args[1];

        if (args.length != 2) {
            usage();
            return;
        }

        System.out.println("Database: "+database);
        System.out.println("Table: " + table);

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.58.137/"+database+"?useCursorFetch=true";
        String user="root";
        String password = "";

        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, password);

            if (!conn.isClosed()) {
                System.out.println("Succeeded connection to the Database!");
                String sql = "select * from " + table;
                conn.setAutoCommit(false);
                PreparedStatement stmt =
                        conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(Integer.MIN_VALUE);
                stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
                //stmt.setMaxRows(100);
                stmt.execute();
                ResultSet rs = stmt.getResultSet();

                String outputFileName = database
                        +"-"+table
                        +"-"+"name-account-sex-age-password-salt-email-qq-sfz-mobile-address-source"
                        +".csv";
                PrintWriter output = new PrintWriter(new BufferedOutputStream(new FileOutputStream(outputFileName)));
                exportData(rs, output);
                output.close();
                rs.close();
                stmt.close();
                conn.close();
            }

        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
