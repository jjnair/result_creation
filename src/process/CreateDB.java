package process;

import java.io.File;
import java.sql.*;

import process.Execute;
public class CreateDB {

	public static void create(Connection con, String filePath) {
		File file = new File(filePath);
		String dbName = file.getName();
		boolean dbexist = false;
		dbName = dbName.substring(0, dbName.length() - 4);
		try {
			Statement st = con.createStatement();
            ResultSet resultSet = con.getMetaData().getCatalogs();
            while(resultSet.next()) {
                if(dbName.equalsIgnoreCase(resultSet.getString(1)))
                dbexist = true;
            }
            if(!dbexist) {
                st.executeUpdate("create database " + dbName);
                System.out.println("Database " + dbName + " Created!!");
                AlternateProcess.dumpSQL(con, filePath);
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}

		AlternateProcess.runQuery(con, filePath);
	}
	
}
