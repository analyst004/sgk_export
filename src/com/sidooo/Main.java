package com.sidooo;
import com.sun.deploy.util.StringUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.*;

public class Main {

    private static void exportData(ResultSet rs, PrintWriter output) {
        try {
            while(rs.next()) {

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

    public static void main(String[] args) {
	    // write your code here

        String database = args[0];
        String table = args[1];

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.18.102.133/" + database;
        String user="root";
        String password = "root";

        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, password);

            if (!conn.isClosed()) {
                System.out.println("Succeeded connection to the Database!");

                Statement statement = conn.createStatement();

                String sql = "select * from " + table;
                ResultSet rs = statement.executeQuery(sql);

                String outputFileName = database
                        +"-"+table
                        +"-"+"name-account-sex-age-password-salt-email-qq-sfz-mobile-address-source"
                        +"-"+".csv";
                PrintWriter output = new PrintWriter(new BufferedOutputStream(new FileOutputStream(outputFileName)));
                exportData(rs, output);
                output.close();
                rs.close();
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
