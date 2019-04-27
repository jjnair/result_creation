package process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.ibatis.common.jdbc.ScriptRunner;
import com.mysql.jdbc.ResultSetMetaData;

public class Execute {
	static ResultSet stepAvg = null;
	static ResultSet stepName = null;
	static ResultSet percentile = null;
	static ResultSet breakdown = null;
    static ResultSet transaction = null;
    static ResultSet pageBreakdown = null;
    static ResultSet script = null;
	
	static HashMap<String, ArrayList<String>> avgResult = new HashMap<String, ArrayList<String>>();
	static HashMap<String, Float> p85 = new HashMap<String, Float>();
	static HashMap<String, Float> p90 = new HashMap<String, Float>();
	static HashMap<String, Float> p95 = new HashMap<String, Float>();
	static ArrayList<String> label = new ArrayList<String>();
	static ArrayList<String> resultAvg;
	
	
	public static void dumpSQL(Connection con, String filePath) {
		File file = new File(filePath);
		String dbName = file.getName();
		dbName = dbName.substring(0, dbName.length() - 4);
		System.out.println(dbName);
		try {
			Statement st = con.createStatement();
			st.executeUpdate("use " + dbName);
			ScriptRunner sr = new ScriptRunner(con, false, false);
			Reader re = new BufferedReader(new FileReader(filePath));
			sr.runScript(re);
			st.executeUpdate("create table Result(Steps mediumtext, Count int, Min float(14,4), Max float(14,4), 85th float(14,4),	90th float(14,4),	95th float(14,4), Avg float(14,4),  Std_Dev float(14,4), Avg_Bytes float(14,4))");
			System.out.println("TABLE RESULT CREATED");
			st.executeUpdate("create table api_breakdown(steps mediumtext, uri mediumtext, avg_resp_time float(14,4), min_resp_time float(14,4), max_resp_time float(14,4),  std_devi_time float(14,4), AvgBytes float(14,4))");
			System.out.println("TABLE api_breakdown CREATED");
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public static void exportToXls(String filePath, Connection con) {
		label.add("Steps");
		label.add("Count");
		label.add("Min");
		label.add("Max");
		label.add("Avg");
		label.add("85th");
		label.add("90th");
		label.add("95th");
		label.add("Std Dev");
		label.add("Avg Bytes");
		
		File file = new File(filePath);
		String dbName = file.getName();
		dbName = dbName.substring(0, dbName.length() - 4);
		String xlsPath = "D:\\testResult\\" + dbName + ".xls";
		int cCount = label.size();
		try {
			Statement resultStatement = con.createStatement();
			resultStatement.executeUpdate("use "+ dbName);
			ResultSet temp =resultStatement.executeQuery("select * from result");
			HSSFWorkbook wb = new HSSFWorkbook();
			HSSFSheet sheet = wb.createSheet(dbName);
			
			System.out.println(cCount);
			HSSFRow row = sheet.createRow(0);
			for (int c = 0; c < cCount; c++) {
				HSSFCell cell = row.createCell(c + 1);
				cell.setCellValue(label.get(c));
			}

			while (temp.next()) {
				row = sheet.createRow(temp.getRow());
				for (int c = 1; c <= cCount; c++) {
					HSSFCell cell = row.createCell(c);
					cell.setCellValue(temp.getString(c));
					System.out.print(temp.getString(c));
				}
			}
			
			File yourFile = new File(xlsPath);
			yourFile.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(xlsPath);
			wb.write(fileOut);
			fileOut.flush();
			fileOut.close();
			wb.close();
			System.out.println("File close....");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void runQuery(Connection con, String filePath) {
		Statement st = null;
		Statement insert = null;
		Statement stSN = null;
		Statement stSA = null;
		Statement breakdownStatement =null;
		File file = new File(filePath);
		String dbName = file.getName();
		dbName = dbName.substring(0, dbName.length() - 4);
		try {
			stSN = con.createStatement();
			stSA = con.createStatement();
			st = con.createStatement();
			breakdownStatement=con.createStatement();
			insert=con.createStatement();
			
			st.executeUpdate("use " + dbName);
			stepName = stSN.executeQuery("select label from step s inner join tx t on t.tx_id = s.tx_id and t.success = 1 group by 1 order by s.step");
			stepAvg = stSA	.executeQuery("SELECT label as 'Step', COUNT(s.time_active/1000) AS 'Count', ROUND(MIN((s.time_active)/1000), 1) AS 'Min', ROUND(MAX((s.time_active)/1000), 1) AS 'Max', ROUND(AVG((s.time_active)/1000), 1) AS 'Avg', ROUND(STD((s.time_active)/1000), 1) AS 'Stddev', ROUND(AVG(s.bytes),1) AS 'Avg Bytes' from step s inner join tx t on t.tx_id = s.tx_id and t.success = 1 group by 1  order by s.step");
			breakdown = breakdownStatement.executeQuery("select s.label, o.path , avg(o.time_active) , min(o.time_active) , max(o.time_active) , std(o.time_active), avg(o.bytes) from  object o    inner join step s on s.step_id = o.step_id and s.step in (1,2,3,4,5) and s.tx_id = o.tx_id    inner join tx t on t.tx_id = s.tx_id and t.success = 1   where o.path like '/Saba/api%'  group by 1, 2 order by s.step, 3 desc");

			while(breakdown.next())
			{
				insert.executeUpdate("insert into api_breakdown values ('"+breakdown.getString(1)+"','"+breakdown.getString(2)+"','"+breakdown.getFloat(3)+"','"+breakdown.getFloat(4)+"','"+breakdown.getFloat(5)+"','"+breakdown.getFloat(6)+"','"+breakdown.getFloat(7)+"')");
			}
		
			ResultSetMetaData rsmd ;
			 rsmd = (ResultSetMetaData) stepAvg.getMetaData();
			int column = rsmd.getColumnCount();
			stepAvg.beforeFirst();
			while (stepName.next()) {
				stepAvg.next();
				ArrayList<String> add = new ArrayList<>();
				for (int i = 1; i <= column; i++) {
					add.add(""+ stepAvg.getString(i) +"");
				}
				avgResult.put(stepName.getString(1), add);
				System.out.println(add.toString());
				insert.executeUpdate("insert into result (Steps, Count, Min, Max, Avg, Std_Dev, Avg_Bytes) values ('"+stepAvg.getString(1)+"',"+stepAvg.getInt(2)+","+stepAvg.getDouble(3)+","+stepAvg.getDouble(4)+","+stepAvg.getDouble(5)+","+stepAvg.getDouble(6)+","+stepAvg.getDouble(7)+" )");
			}
			
			stepName.beforeFirst();

			while (stepName.next()) {
				percentile = st.executeQuery("SELECT   SUM(g1.r) sr,g2.time_active l, SUM(g1.r)/(SELECT COUNT(*) FROM step WHERE label = '"+stepName.getString(1)+"' ) p	FROM    (SELECT COUNT(*) r, time_active FROM step WHERE label = '"+stepName.getString(1)+"' GROUP BY time_active) g1	JOIN    (SELECT COUNT(*) r, time_active FROM step WHERE label = '"+stepName.getString(1)+"' GROUP BY time_active) g2	ON       g1.time_active < g2.time_active	GROUP BY g2.time_active	HAVING p > 0.85	ORDER BY p	LIMIT 1");
				if(percentile.next()){
				float per85 = ((float) percentile.getInt(2) / 1000);
				p85.put(stepName.getString(1), per85);
				insert.executeUpdate("update result set 85th ="+per85+" where Steps = '"+stepName.getString(1)+"' ");
				}
				else{
					p85.put(stepName.getString(1), (float) 0);
					insert.executeUpdate("update result set 85th =0 where Steps = '"+stepName.getString(1)+"' ");
				}
			}
			stepName.beforeFirst();

			while (stepName.next()) {
				percentile = st.executeQuery("SELECT   SUM(g1.r) sr,g2.time_active l, SUM(g1.r)/(SELECT COUNT(*) FROM step WHERE label = '"+stepName.getString(1)+"') p	FROM    (SELECT COUNT(*) r, time_active FROM step WHERE label = '"+stepName.getString(1)+"' GROUP BY time_active) g1	JOIN    (SELECT COUNT(*) r, time_active FROM step WHERE label = '"+stepName.getString(1)+"' GROUP BY time_active) g2	ON       g1.time_active < g2.time_active	GROUP BY g2.time_active	HAVING p > 0.90	ORDER BY p	LIMIT 1");
				if(percentile.next()){
					float per90 = ((float) percentile.getInt(2) / 1000);
					p90.put(stepName.getString(1), per90);
					insert.executeUpdate("update result set 90th ="+per90+" where Steps = '"+stepName.getString(1)+"' ");
					}
					else{
						p90.put(stepName.getString(1), (float) 0);
						insert.executeUpdate("update result set 90th =0 where Steps = '"+stepName.getString(1)+"' ");
					}
			}
			stepName.beforeFirst();
			while (stepName.next()) {
				percentile = st.executeQuery("SELECT   SUM(g1.r) sr,g2.time_active l, SUM(g1.r)/(SELECT COUNT(*) FROM step WHERE label = '"+stepName.getString(1)+"') p	FROM    (SELECT COUNT(*) r, time_active FROM step WHERE label = '"+stepName.getString(1)+"' GROUP BY time_active) g1	JOIN    (SELECT COUNT(*) r, time_active FROM step WHERE label = '"+stepName.getString(1)+"' GROUP BY time_active) g2	ON       g1.time_active < g2.time_active	GROUP BY g2.time_active	HAVING p > 0.95	ORDER BY p	LIMIT 1");
				if(percentile.next()){
					float per95 = ((float) percentile.getInt(2) / 1000);
					p95.put(stepName.getString(1), per95);
					insert.executeUpdate("update result set 95th ="+per95+" where Steps = '"+stepName.getString(1)+"' ");
					}
					else{
						p95.put(stepName.getString(1), (float) 0);
						insert.executeUpdate("update result set 95th =0 where Steps = '"+stepName.getString(1)+"' ");
					}
			}
			stepName.beforeFirst();

		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
            stepName.beforeFirst();
            Statement script_id = con.createStatement();
            Statement tx_id = con.createStatement();
            Statement pagebreak = con.createStatement();
           HashMap<String,String> ScriptTxMap = new HashMap<String,String>();
            script =  script_id.executeQuery("select scripts.script_id,scripts.title from scripts");
                 while (script.next())
                    {
                        transaction  =  tx_id.executeQuery("select tx_id,st.title from tx t right join scripts st on t.script_id = st.script_id where t.success=1 and st.script_id='"+script.getString("script_id")+"'");
                        ScriptTxMap.put(script.getString("script_id"),transaction.getString("tx_id"));
                     }

                   for(Map.Entry<String,String> en : ScriptTxMap.entrySet()){
                       pageBreakdown = pagebreak.executeQuery("select  step.label,object.path, object.url ,step.tx_id,object.time_active,tx.script_id,st.script_name from step \n" +
                               "   left outer join object \n" +
                               " \t\ton step.tx_id=object.tx_id and step.step_id= object.step_id \n" +
                               "\tleft outer join tx \n" +
                               "\t   on tx.tx_id = step.tx_id \n" +
                               "\tleft outer join scripts st\n" +
                               "\t   on tx.script_id = st.script_id\n" +
                               "where tx.success=1 and tx.tx_id='"+en.getValue()+"' order by st.script_name,step.tx_id,step.label");
                   }


        }catch(Exception e){

        }

    }



}
