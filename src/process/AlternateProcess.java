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

import org.apache.poi.hssf.record.PageBreakRecord;
import org.apache.poi.hssf.usermodel.*;

import com.ibatis.common.jdbc.ScriptRunner;
import com.mysql.jdbc.ResultSetMetaData;
import org.apache.poi.ss.usermodel.CellStyle;

public class AlternateProcess   {
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
/*
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
    }*/

    public static void runQuery(Connection con, String filePath) {
        Statement st = null;
        Statement insert = null;
        Statement stSN = null;
        Statement stSA = null;
        Statement breakdownStatement =null;

        HSSFWorkbook wb = new HSSFWorkbook();
        File file = new File(filePath);
        String dbName = file.getName();
        dbName = dbName.substring(0, dbName.length() - 4);
        String xlsPath = "D:\\testResult\\" + dbName + ".xls";

        try {
            ArrayList<String>label = new ArrayList<String>();
            label.add("Steps");
            label.add("ScriptName");
            label.add("Count");
            label.add("Min");
            label.add("Max");
            label.add("Avg");
            label.add("Std Dev");
            label.add("Avg Bytes");
            label.add("85th");
            label.add("90th");
            label.add("95th");


            stSN = con.createStatement();
            stSA = con.createStatement();
            st = con.createStatement();
            breakdownStatement=con.createStatement();
            insert=con.createStatement();

            st.executeUpdate("use " + dbName);
            stepName = stSN.executeQuery("select label from step s inner join tx t on t.tx_id = s.tx_id and t.success = 1 group by 1 order by s.step");
            stepAvg = stSA	.executeQuery("SELECT \n" +
                    "label as 'Step',\n" +
                    "scripts.script_name,\n" +
                    "COUNT(step.time_active/1000) AS 'Count', \n" +
                    "ROUND(MIN((step.time_active)/1000), 1) AS 'Min', \n" +
                    "ROUND(MAX((step.time_active)/1000), 1) AS 'Max', \n" +
                    "ROUND(AVG((step.time_active)/1000), 1) AS 'Avg', \n" +
                    "ROUND(STD((step.time_active)/1000), 1) AS 'Stddev', \n" +
                    "ROUND(AVG(step.bytes),1) AS 'Avg Bytes'\n" +
                    "from step  inner join tx on tx.tx_id = step.tx_id \n" +
                    "INNER JOIN scripts ON tx.script_id = scripts.script_id and tx.success = 1 GROUP BY 1 ORDER BY 1 ");
            breakdown = breakdownStatement.executeQuery("SELECT s.label, o.path, AVG(o.time_active), MIN(o.time_active), MAX(o.time_active), STD(o.time_active), AVG(o.bytes)\n" +
                    "FROM object o\n" +
                    "INNER JOIN step s ON s.step_id = o.step_id AND s.step IN (1,2,3,4,5) AND s.tx_id = o.tx_id\n" +
                    "INNER JOIN tx t ON t.tx_id = s.tx_id AND t.success = 1\n" +
                    "WHERE o.path LIKE '/Saba/api%'\n" +
                    "GROUP BY 1, 2\n" +
                    "ORDER BY s.step, 3 DESC");


            HSSFSheet sheet = wb.createSheet("Overall");
            int cCount = stepAvg.getMetaData().getColumnCount();

            HSSFRow row = sheet.createRow(0);
            for (int c = 0; c < label.size(); c++) {
                HSSFCell cell = row.createCell(c + 1);
                cell.setCellValue(label.get(c));
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
            while (stepAvg.next()) {
                row = sheet.createRow(stepAvg.getRow());
                for (int c = 1; c <= cCount; c++) {
                    HSSFCell cell = row.createCell(c);
                    cell.setCellValue(stepAvg.getString(c));
                    System.out.print(stepAvg.getString(c));

                }
                if(stepName.next()) {
                    HSSFCell cell85 = row.createCell(cCount + 1);
                    cell85.setCellValue(p85.get(stepAvg.getString(1)));
                    HSSFCell cell90 = row.createCell(cCount + 2);
                    cell90.setCellValue(p90.get(stepAvg.getString(1)));
                    HSSFCell cell95 = row.createCell(cCount + 3);
                    cell95.setCellValue(p95.get(stepAvg.getString(1)));
                }
                sheet.createRow(sheet.getLastRowNum()+1);

            }
            int currentPosition = sheet.getLastRowNum();
            currentPosition= currentPosition+10;
            Statement errorSummaryQuery = con.createStatement();
            ArrayList<String> errorlabel = new ArrayList<String>();
            ResultSet QueryResult=null;
            QueryResult = errorSummaryQuery.executeQuery("select tx.err_msg , count(tx.err_msg) ,tx.script_id,scripts.script_name \n" +
                    "from tx right join scripts on tx.script_id = scripts.script_id \n" +
                    "group by tx.err_msg order by scripts.script_name");

            errorlabel.add("Error Message");
            errorlabel.add("Error Count");
            errorlabel.add("Script ID");
            errorlabel.add("Script Name");
            HSSFRow rowerror = sheet.createRow(++currentPosition);
            for(int k=0;k<errorlabel.size();k++) {
                   HSSFCell errorcell = rowerror.createCell(k+1);
                   errorcell.setCellValue(errorlabel.get(k));
            }

            cCount = QueryResult.getMetaData().getColumnCount();
            while (QueryResult.next()){
                row = sheet.createRow(currentPosition++  );
                for (int c = 1; c <= cCount; c++) {
                    HSSFCell cell = row.createCell(c);
                    cell.setCellValue(QueryResult.getString(c));
                    System.out.print(QueryResult.getString(c));

                }
            }



            stepName.beforeFirst();
            Statement script_id = con.createStatement();
            Statement tx_id = con.createStatement();
            Statement pagebreak = con.createStatement();
            HashMap<String,String> ScriptTxMap = new HashMap<String,String>();
            HSSFSheet sheet1 = wb.createSheet("Breakdown");
            script =  script_id.executeQuery("select scripts.script_id,scripts.title from scripts");
            HSSFRow rowbreakdown=null;
             int rowcounter=1;
            while (script.next())
            {
                   transaction  =  tx_id.executeQuery("select tx_id,st.title from tx t right join scripts st on t.script_id = st.script_id where t.success=1 and st.script_id='"+script.getString("script_id")+"'");

                   if(transaction.next()) {
                       ScriptTxMap.put(script.getString("script_id"), transaction.getString("tx_id"));
                   }
            }
            ArrayList<String> label1 = new ArrayList<String>();
            label1.add("Step");
            label1.add("Path");
            label1.add("URL");
            label1.add("Transaction");
            label1.add("Time Active");
            label1.add("Script ID");
            label1.add("Script Name");

            HSSFRow row1 = sheet1.createRow(rowcounter++);
            for (int c = 0; c < label1.size(); c++) {
                HSSFCell cell = row1.createCell(c+1 );
                cell.setCellValue(label1.get(c));
            }


            for(Map.Entry<String,String> en : ScriptTxMap.entrySet()) {

                pageBreakdown = pagebreak.executeQuery("select  step.label,object.path, object.url ,step.tx_id,object.time_active,tx.script_id,st.script_name from step \n" +
                        "   left outer join object \n" +
                        " \t\ton step.tx_id=object.tx_id and step.step_id= object.step_id \n" +
                        "\tleft outer join tx \n" +
                        "\t   on tx.tx_id = step.tx_id \n" +
                        "\tleft outer join scripts st\n" +
                        "\t   on tx.script_id = st.script_id\n" +
                        "where tx.success=1 and tx.tx_id='" + en.getValue() + "' order by st.script_name,step.tx_id,step.label");


                int columns = pageBreakdown.getMetaData().getColumnCount();
                while (pageBreakdown.next()) {
                    rowbreakdown = sheet1.createRow(rowcounter++);
                    for (int j = 1; j <= columns; j++) {
                        HSSFCell cell = rowbreakdown.createCell(j);
                        cell.setCellValue(pageBreakdown.getString(j));
                        System.out.print(pageBreakdown.getString(j)+"\n");
                    }

                }
                sheet1.createRow(rowcounter++);
            }


            File yourFile = new File(xlsPath);
            yourFile.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(xlsPath);
            wb.write(fileOut);
            fileOut.flush();
            fileOut.close();
            pageBreakdown.close();
            transaction.close();
            wb.close();

        }catch(Exception e){
                e.printStackTrace();
        }finally {

            System.out.println("File close....");
        }



    }



}
