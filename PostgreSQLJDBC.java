package Bigdata;

import java.sql.DriverManager;

public class PostgreSQLJDBC {
   public static void main(String args[]) {
      try {
         Class.forName("org.postgresql.Driver");
         DriverManager
               .getConnection("jdbc:postgresql://localhost/Uber",
                     "postgres", "12345");
      } catch (Exception e) {
         e.printStackTrace();
         System.err.println(e.getClass().getName() + ": " + e.getMessage());
         System.exit(0);
      }
      System.out.println("Opened database successfully");
   }
}