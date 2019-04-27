package DBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import process.AlternateProcess;
import process.CreateDB;
import process.Execute;

public class DB_Connect {

	public static  Connection connect() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager
					.getConnection(
							"jdbc:mysql://localhost:3306/?autoReconnect=true&useSSL=false","root", "root123");
			System.out.println("Connected to localhost:3300");
			return con;
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			Connection con = null;
			return con;
		}
	}
	

	
	public static void main(String[] args) {
		String filePath = "D:\\RAS_test_Results\\SabaSearch_Phase2\\SC_Search_Indexing_Test2\\SC_Search_Indexing_Test2.sql";
	
		Connection con = connect();
		CreateDB.create( con, filePath);
		System.out.println("MAIN END");
	}

}
