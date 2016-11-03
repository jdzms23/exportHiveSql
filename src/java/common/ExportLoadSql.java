package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ExportLoadSql {

	public void exportLoadSql(String date) {
		try {
	        Class.forName( "org.postgresql.Driver" ).newInstance();
	        String url = "jdbc:postgresql://192.168.9.129:7432/hive";
	        Connection con = DriverManager.getConnection(url, "hive" , "kNhRy65t9s" );
	        Statement st = con.createStatement();
	        String sql = "SELECT T1." +"\"" + "TBL_NAME" +"\"" + ", U.*, U1." +"\"" + "COLUMNS" +"\"" + ", U1." +"\"" + "COLUMNSTYPE" +"\"" + ", U2." +"\"" + "PARTS" +"\"" + "" +
	     		  " FROM (SELECT T." +"\"" + "TBL_ID" +"\"" + "," + 
	     	               "group_concat(P." +"\"" + "PKEY_NAME" +"\"" + ") AS " +"\"" + "PKEYNAMES" +"\"" + "," +
	     	               "group_concat(P." +"\"" + "PKEY_TYPE" +"\"" + ") AS " +"\"" + "PKEYTYPES" +"\"" + " " +
	     	          " FROM " +"\"" + "TBLS" +"\"" + " T LEFT JOIN " +"\"" + "PARTITION_KEYS" +"\"" + " P " +
	     	         " ON( T." +"\"" + "TBL_ID" +"\"" + " = P." +"\"" + "TBL_ID" +"\"" + ") " +
	     	         " GROUP BY T." +"\"" + "TBL_ID" +"\"" + ") U, " +
	     	       " (SELECT group_concat(V." +"\"" + "COLUMN_NAME" +"\"" + ") AS " +"\"" + "COLUMNS" +"\"" + ", " +
	     	              " group_concat(V." +"\"" + "TYPE_NAME" +"\"" + ") AS " +"\"" + "COLUMNSTYPE" +"\"" + ", " +
	     	              " T." +"\"" + "TBL_ID" +"\"" + " " +
	     	          " FROM " +"\"" + "COLUMNS_V2" +"\"" + " V, " +"\"" + "CDS" +"\"" + " C, " +"\"" + "SDS" +"\"" + " S, " +"\"" + "TBLS" +"\"" + " T " +
	     	         " WHERE V." +"\"" + "CD_ID" +"\"" + " = C." +"\"" + "CD_ID" +"\"" + " " +
	     	          " AND C." +"\"" + "CD_ID" +"\"" + " = S." +"\"" + "CD_ID" +"\"" + " " +
	     	          " AND S." +"\"" + "SD_ID" +"\"" + " = T." +"\"" + "SD_ID" +"\"" + " " +
	     	          " GROUP BY T." +"\"" + "TBL_ID" +"\"" + ") U1, " +
	     	       " (SELECT T." +"\"" + "TBL_ID" +"\"" + ", GROUP_CONCAT(P." +"\"" + "PART_NAME" +"\"" + ") AS " +"\"" + "PARTS" +"\"" + " " +
	     	        "  FROM " + "\"" + "TBLS" + "\"" + "T LEFT JOIN " +"\"" + "PARTITIONS" +"\"" + " P ON( T." + "\"" + "TBL_ID" + "\"" +
	     	        " = p." + "\"" + "TBL_ID" + "\"" + ")" +
	     	        " GROUP BY T." +"\"" + "TBL_ID" +"\"" + ") U2, " +
	     	       " " +"\"" + "TBLS" +"\"" + " T1 " +
	     	" WHERE U." +"\"" + "TBL_ID" +"\"" + " = U1." +"\"" + "TBL_ID" +"\"" + " " +
	     	 "  AND U." +"\"" + "TBL_ID" +"\"" + " = T1." +"\"" + "TBL_ID" +"\"" + " " +
	     	 "  AND T1." +"\"" + "TBL_ID" +"\"" + " = U2." +"\"" + "TBL_ID" +"\"";
	        
	        System.out.println(sql);
	        
	        
	        ResultSet rs = st.executeQuery(sql);
            List<TableTO> tableTo = new ArrayList<TableTO>();
           
            while (rs.next())
            {
            	TableTO to = new TableTO();
            	to.setTableName(rs.getString(1));
            	to.setPkNames(rs.getString(3));
            	to.setPkTypes(rs.getString(4));
            	to.setColumns(rs.getString(5));
            	to.setColumnsType(rs.getString(6));
            	to.setParts(rs.getString(7));
            	tableTo.add(to);
            	
           } 
	            
            File file = new File(ExportHiveSql.class.getClassLoader().getResource("").getPath() + "loadsql.sql");
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            String srcWareHouse = "hftp://dace-hdfs-233.jh:50070/user/hive/warehouse/";
            String destWareHouse = "hdfs://dace-hdfs-3-245.jh.hupu.com:8020/user/hive/warehouse/";
      	    
            StringBuffer sb = new StringBuffer();
      	    sb.append("#! /bin/bash" + "\n");
      	    sb.append("export HADOOP_USER_NAME=hdfs" + "\n");
      	    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      	   
            for (int i = 0; i < tableTo.size(); i++) {
         	   TableTO to = tableTo.get(i);
         	   String lastDt = "";
         	   if (to.getParts() == null) {
         		   //load数据
         		   sb.append("hadoop distcp -m 100 ").append(srcWareHouse)
         		    .append(to.getTableName())
         		    .append(" ").append(destWareHouse)
         		    .append(to.getTableName());
         	   } else {
             	   String[] parts = to.getParts().split(",");
             	   sb.append("hadoop distcp -m 100 ");
             	   //load分区数据
             	   for (int y = 0; y < parts.length; y++) {
             		   String part= parts[y];
             		   if (part.contains("/")) {
             			   part = part.substring(0, part.indexOf("/"));
             		   }
             		   if (part.contains("=")) {
             			   String partitionDate = part.substring(part.indexOf("=") + 1);
             			   Date dateD = sdf.parse(date);
             			   Date partitionD = sdf.parse(partitionDate);
             			   
             			   if (partitionD.getTime() > dateD.getTime()) {
             				   continue;
             			   }
             		   }
             		   if (lastDt.equals(part)) {
             			   continue;
             		   } else {
                 		   sb.append(srcWareHouse).append(to.getTableName()).append("/")
                 		   .append(part.replaceAll("-", "")).append(" ");             			   
             		   }

             		  lastDt = part;
             	   }
         		   sb.append(destWareHouse).append(to.getTableName()).append("\n");
         	   }

         	   writer.write(sb.toString() + "\n");
         	  sb = new StringBuffer();
            }
            writer.close();
            rs.close();
            st.close();
            con.close();
		} catch (Exception e) {
			
		}
	}
	public static void main(String[] args) {
		ExportLoadSql loadSql = new ExportLoadSql();
		loadSql.exportLoadSql("20150324");
	}
}
