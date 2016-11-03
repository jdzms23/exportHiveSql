package common;

import  org.postgresql.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ExportHiveSql {

	private ResultSet rs = null;
	
	public ExportHiveSql() throws Exception {
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
        rs = st.executeQuery(sql);
	}
	
	public void dropTable() {

		String[] tables = {"rd_e_wap_event"
				,
				"rd_e_wap"
				,
				"rd_e_wal_kq"
				,
				"rd_e_pal"
				,
				"rd_e_custom_event"
				,
				"rd_e_app_terminate"
				,
				"rd_e_app_header"
				,
				"rd_e_app_event"
				,
				"rd_e_app_error"
				,
				"rd_e_app_apps"
				,
				"rd_e_ad_topn"
				,
				"rd_e_ad"
				,
				"rd_b_wl_app_kq_interactlog"
				,
				"rd_b_wl_app_kq_clientjoin"
				,
				"rd_b_wl_app_downloadlog"
				,
				"rd_b_web_biz_sh_taobaoshop"
				,
				"rd_b_web_biz_sh_taobaoitem"
				,
				"rd_b_web_biz_sh_taobaocustom"
				,
				"rd_b_basic_common_ipdata_1"
				,
				"rd_b_basic_common_ipdata"
				,
				"dual"
				,
				"dd_wc_applabel"
				,
				"dd_wa18_kq_user_channel"
				,
				"dd_wa18_kq_quiz_detail_all"
				,
				"dd_wa18_kq_quiz_detail"
				,
				"dd_wa18_kq_pay_daily_all"
				,
				"dd_wa18_kq_pay_daily"
				,
				"dd_wa18_kq_finance_type_all"
				,
				"dd_wa18_kq_finance_type"
				,
				"dd_wa18_kq_finance_log"
				,
				"dd_u1_g_client_user_info"
				,
				"dd_sns_log_m"
				,
				"dd_sns_log"
				,
				"dd_sns_dailyusers_type_pc"
				,
				"dd_sns_dailyusers_type_m"
				,
				"dd_sns_dailyusers_pc"
				,
				"dd_sns_dailyusers_m"
				,
				"dd_sns_dailyusers_action_plus"
				,
				"dd_sns_dailyusers_act_type"
				,
				"dd_b_wlapp_wa8_gamejoin"
				,
				"dd_b_wlapp_wa22_visit_outgame"
				,
				"dd_b_wlapp_wa22_visit_ingame"
				,
				"dd_b_wlapp_wa22_interaction_outgame"
				,
				"dd_b_wlapp_wa22_interaction_ingame"
				,
				"dd_b_wlapp_wa16_second_pvuv"
				,
				"dd_b_wlapp_wa13_visit_distinct"
				,
				"dd_b_wlapp_wa13_visit"
				,
				"dd_b_wl_app_kq_lid"
				,
				"dd_b_pf_fdt_pspt_ucloginlog"
				,
				"dd_b_lt_l5_usersource"
				,
				"dd_b_lt_l5_all_visit_source"
				,
				"dd_b_lt_l5_all_visit"
				,
				"dd_b_lt_l4_userurl"
				,
				"dd_b_lt_l4_userlist_day"
				,
				"dd_b_game_g7_log"
				,
				"dd_b_game_g22_mobile_game"
				,
				"dd_b_game_g20_onlinelog"
				,
				"dd_b_game_g20_duration"
				,
				"dd_b_game_g16_log"
				,
				"dd_b_game_g10_login"
				,
				"dd_b_ec_shlog_m_event"
				,
				"dd_b_ec_shlog_m"
				,
				"dd_b_ec_shlog"
				,
				"dd_b_ec_e4_vst"
				,
				"dd_b_ec_e4_go_click"
				,
				"dd_b_ec_e4_1"
				,
				"dd_b_ec_e3_sns"
				,
				"dd_b_ec_e2_sh_nosellers"
				,
				"dd_b_ec_e2_sh_noseller"
				,
				"dd_b_ec_e2_sh_all"
				,
				"dd_b_ec_e2_seller_hupuid"
				,
				"dd_b_ec_e2"
				,
				"dd_b_ec_e19_link"
				,
				"dd_b_ec_e17_visit"
				,
				"dd_b_ec_e17_userlist"
				,
				"dd_b_ec_e17_click"
				,
				"dd_b_ec_e16_userparse_ture"
				,
				"dd_b_ec_e16_userparse_all"
				,
				"dd_b_ec_e16_daily_click"
				,
				"dd_b_ec_e15_orderlink"
				,
				"dd_b_ec_e14_link"
				,
				"dd_b_ec_dailyuser_currentlast"
				,
				"dd_b_ec_dailyuser_behavior_m"
				,
				"dd_b_ec_dailyuser_behavior"
				,
				"dd_b_ec_ad_vidvsturl_m"
				,
				"dd_b_ec_ad_vidvsturl"
				,
				"dd_b_ec_ad_raw_uid_m"
				,
				"dd_b_ec_ad_raw_uid"
				,
				"dd_b_ec_ad_raw_m"
				,
				"dd_b_ec_ad_raw"
				,
				"dd_b_basic_u3_web_user_profile"
				,
				"dd_b_basic_u3_interim_web_daily"
				,
				"dd_b_basic_u2_wap_user_profile"
				,
				"dd_b_basic_u2_interim_wap_daily"
				,
				"dd_b_basic_u1_interim_groupby_appdevice"
				,
				"dd_b_basic_u1_interim_app"
				,
				"dd_b_basic_u1_groupby_appdevice"
				,
				"dd_b_basic_u1_app_user_viscosity_week"
				,
				"dd_b_basic_u1_app_user_viscosity_month"
				,
				"dd_b_basic_u1_app_user_profile"
				,
				"dd_b_basic_sns_b37"
				,
				"dd_b_basic_pvuv"
				,
				"dd_b_basic_common_visits_login"
				,
				"dd_b_basic_common_visits_all"
				,
				"dd_b_basic_common_visits"
				,
				"dd_b_basic_common_userid"
				,
				"dd_b_basic_common_url"
				,
				"dd_b_basic_common_unique"
				,
				"dd_b_basic_common_newreg_user_list"
				,
				"dd_b_basic_common_newreg_effective_user_list"
				,
				"dd_b_basic_common_login_vid"
				,
				"dd_b_basic_common_hupunew_user_list"
				,
				"dd_b_basic_common_game_newreg_user_list"
				,
				"dd_b_basic_common_game_main_user_list"
				,
				"dd_b_basic_common_effective_user_list"
				,
				"dd_b_basic_common_daily_login"
				,
				"dd_b_basic_common_daily_all"
				,
				"dd_b_basic_b8"
				,
				"dd_b_basic_b7"
				,
				"dd_b_basic_b61_newuser"
				,
				"dd_b_basic_b60_session"
				,
				"dd_b_basic_b57_referurl"
				,
				"dd_b_basic_b57_gourl"
				,
				"dd_b_basic_b50_videovisit"
				,
				"dd_b_basic_b49_newsvisit"
				,
				"dd_b_basic_b48_snstype_all_vid"
				,
				"dd_b_basic_b48_snstype_all_lgn"
				,
				"dd_b_basic_b47_url"
				,
				"dd_b_basic_b47_outurl"
				,
				"dd_b_basic_b47_inurl"
				,
				"dd_b_basic_b46_searchkeyword"
				,
				"dd_b_basic_b45_unique"
				,
				"dd_b_basic_b45_login_unique"
				,
				"dd_b_basic_b44_searchkey"
				,
				"dd_b_basic_b44_searchdetail"
				,
				"dd_b_basic_b39_vidlist"
				,
				"dd_b_basic_b39_effectiveuser"
				,
				"dd_b_basic_b39_bytimes_sid"
				,
				"dd_b_basic_b38_unlogin_uv"
				,
				"dd_b_basic_b38_login_uv"
				,
				"dd_b_basic_b36_user_day"
				,
				"dd_b_basic_b36_url"
				,
				"dd_b_basic_b33_level"
				,
				"dd_b_basic_b18_key"
				,
				"dd_b_basic_b18"
				,
				"dd_b_basic_b17"
				,
				"dd_b_basic_b16_url"
				,
				"dd_b_basic_ad_pc_visit"
				,
				"dd_b_basic_ad_pc_all_visit"
				,
				"dd_b_basic_ad_m_visit"
				,
				"dd_b_basic_ad_m_all_visit"
				,
				"dd_b_advert_ad2_dfp_click"};
		ArrayList<String> tableAry = new ArrayList<String>(Arrays.asList(tables));
		
        try 
         {
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
            File file = new File(ExportHiveSql.class.getClassLoader().getResource("").getPath() + "dropsql.sql");
	           BufferedWriter writer = new BufferedWriter(new FileWriter(file));
	           for (int i = 0; i < tableTo.size(); i++) {
	        	   TableTO to = tableTo.get(i);
	        	   StringBuffer sb = new StringBuffer();
	
	        	   //删除当前存在表
	        	   if (!tableAry.contains(to.getTableName())) {
	        		   sb.append("DROP TABLE IF EXISTS ").append(to.getTableName()).append(";\n");
		        	   writer.write(sb.toString() + "\n");
	        	   }
	        	  
	           }
	           writer.close();
	           rs.close();
	           
         }catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	public void generateSql() {
        try 
         {
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
           
           File file = new File(ExportHiveSql.class.getClassLoader().getResource("").getPath() + "sql.sql");
           BufferedWriter writer = new BufferedWriter(new FileWriter(file));
           for (int i = 0; i < tableTo.size(); i++) {
        	   TableTO to = tableTo.get(i);
        	   String[] colums = to.getColumns().split(",");
        	   StringBuffer sb = new StringBuffer();

        	   //删除当前存在表
        	   sb.append("DROP TABLE ").append(to.getTableName()).append(";\n");
        	   
        	   //创建表
        	   sb.append("CREATE TABLE " + to.getTableName() + "(");
        	   if (!to.getColumnsType().contains("map<")) {
	        	   String[] columnsType = to.getColumnsType().split(",");
	        	   
	        	   for (int j = 0; j < colums.length; j++) {
	        		   if (j == colums.length - 1) {
	        			   sb.append(colums[j]).append(" ").append(columnsType[j]);
	        		   } else {
	        			   sb.append(colums[j]).append(" ").append(columnsType[j]).append(",");
	        		   }
	        	   }
        	   } else {
        		   String[] columnsType = to.getColumnsType().split(",");
        		   ArrayList<Integer> depreIndex = new ArrayList<Integer>();
        		   
        		   //判断是否有map类型
        		   for (int x = 0; x < columnsType.length; x++) {
        			   if (columnsType[x].contains("map<")) {
        				   columnsType[x] = columnsType[x] + "," + columnsType[x+1];
        				   depreIndex.add(x+1);
        			   }
        		   }
        		   LinkedList<String> columnsTypeAry = new LinkedList<String>(Arrays.asList(columnsType));
        		   
        		   for (int y = depreIndex.size() - 1; y >= 0; y--) {
        			   columnsTypeAry.remove(depreIndex.get(y).intValue());
        			   
        		   }
        		   
	        	   for (int j = 0; j < colums.length; j++) {
	        		   if (j == colums.length - 1) {
	        			   sb.append(colums[j]).append(" ").append(columnsTypeAry.get(j));
	        		   } else {
	        			   sb.append(colums[j]).append(" ").append(columnsTypeAry.get(j)).append(",");
	        		   }
	        	   }
        	   }
        	   
        	   //分区
        	   if (to.getParts() == null) {
        		   sb.append(");");
        		   
        		   //load数据
//        		   sb.append(" load data inpath '/user/hive/warehouse/")
//        		    .append(to.getTableName()).append("/")
//        		    .append("'")
//        		    .append(" overwrite into table ")
//        		    .append(to.getTableName()).append(";");
        	   } else {
        		   String[] pkNams = to.getPkNames().split(",");
            	   String[] pkTypes = to.getPkTypes().split(",");
            	   String[] parts = to.getParts().split(",");
            	   
        		   sb.append(") PARTITIONED BY (");
            	   for (int k = pkNams.length -1 ; k >= 0; k--) {
            		   if (k == 0) {
            			   sb.append(pkNams[k]).append(" ").append(pkTypes[k]);
            		   } else {
            			   sb.append(pkNams[k]).append(" ").append(pkTypes[k]).append(",");
            		   }
            	   }
            	   sb.append(");");

            	   //修改分区
            	   sb.append("\n" + "ALTER TABLE " + to.getTableName() + " ADD");
            	   for (int x = 0; x < parts.length; x++) {
            		   String part = parts[x];
            		   sb.append(" PARTITION " + " (");
            		   if (part.contains("/")) {
            			   sb.append(part.replaceAll("/", ",").replaceAll("-", "")).append(") ");
            		   } else {
            			   sb.append(part).append(")");
            		   }
            	   }
            	   sb.append(";");
            	   
            	   //load分区数据
//            	   for (int y = 0; y < parts.length; y++) {
//            		   String part= parts[y];
//            		   sb.append("\n" + " load data inpath '/user/hive/warehouse/")
//            		   .append(to.getTableName()).append("/")
//            		   .append(part.replaceAll("-", "")).append("'")
//            		   .append(" overwrite into table ")
//            		   .append(to.getTableName());
//            		   if (part.contains("/")) {
//            			   sb.append(" partition(").append(part.replaceAll("/", "',").replaceAll("-", "").replaceAll("=", "='")).append("');");
//            		   } else {
//            			   sb.append(" partition(").append(part.replaceAll("=", "='")).append("');");
//            		   }
//            		   
//            	   }
        	   }
        	   writer.write(sb.toString() + "\n");
           }
           writer.close();
           rs.close();
       } catch (Exception e)
        {
        	e.printStackTrace();
       } 
	    
	}
	public   static   void  main(String args[]) throws Exception
    {
		ExportHiveSql exportHiveSql = new ExportHiveSql();
		exportHiveSql.generateSql();
    }
}
