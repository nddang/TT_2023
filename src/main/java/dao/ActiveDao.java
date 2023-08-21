package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ActiveDao {
	public void them(List<String[]> lines) throws SQLException, ParseException {
		String query="INSERT INTO `active`(`Perceived_Severity`, `Alarm_Type`, `NE`, `Location`,"
				+ " `Additional_NE`, `Additional_Location`, `Alarm_Code`, `Specific_Problem`, "
				+ "`Raised_Time`, `Changed_Time`, `NE_Type`, `Remark`, `ACK_State`, `(Un)ACK_Time`, "
				+ "`(Un)ACK_User_ID`, `(Un)ACK_System_ID`, `Comment_Time`, `Comment`, "
				+ "`Comment_User_ID`, `Comment_System_ID`, `Product`, `Alarm_Aid`, `Alarm_ID`, "
				+ "`NE_IP`, `Link`, `NE_Group`, `NE_Agent`, `System_Type`, `Additional_Information`, "
				+ "`Path_Name`, `Custom_Attribute_1`, `Custom_Attribute_2`, `Custom_Attribute_3`, "
				+ "`Custom_Attribute_4`, `Custom_Attribute_5`, `Custom_Attribute_6`, `Custom_Attribute_7`,"
				+ " `Custom_Attribute_8`, `Custom_Attribute_9`, `Custom_Attribute_10`,`Status`)"
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'Active')";
		
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}
//		System.out.println("---------------------");
		conn.setAutoCommit(false); 
		PreparedStatement st=conn.prepareStatement(query);
		int dong=0;
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for(String[] line : lines) {
			if(dong>=2) {
				int i=1;
				for(String x : line) {
					try {
						switch (i ) {
						//date time
					    	case  9:
					    		if(x=="") st.setDate(i, null);
					    		else {
					    			Date date = formatter.parse(x);
					    			st.setString(i, formatter.format(date));
					    		}
					    		break;
					    	case  10:
					    		if(x=="") st.setDate(i, null);
					    		else {
					    			Date date = formatter.parse(x);
					    			st.setString(i, formatter.format(date));
					    		}
					    		break;
					    	case  14:
					    		if(x=="") st.setDate(i, null);
					    		else {
					    			Date date = formatter.parse(x);
					    			st.setString(i, formatter.format(date));
					    		}
					    		break;
					    	case  17:
					    		if(x=="") st.setDate(i, null);
					    		else {
					    			Date date = formatter.parse(x);
					    			st.setString(i, formatter.format(date));
					    		}
					    		break;
					    	default:
					    		st.setString(i, x);
//								System.out.println(x);
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
					i++;
				}
				st.addBatch();
//				System.out.println("---------------------");
			}
			dong++;
		}
		st.executeBatch();
		conn.commit();
		st.close();
		conn.close();
		System.out.println("them thanh cong "+(dong-2)+" dong");
		System.out.println("---------------------");
	}
	
	public void capnhat_trangthai() throws SQLException {
		String query1="UPDATE active, history set active.Status='Cleared'\r\n"
				+ "WHERE active.NE=history.NE and active.Alarm_Code=history.Alarm_Code and active.Raised_Time=history.Raised_Time AND history.Cleared_Time IS NOT NULL;";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}
		PreparedStatement st=conn.prepareStatement(query1);
		st.execute();
		st.close();
		conn.close();
		System.out.println("cap nhat trang thai thanh cong");
		System.out.println("---------------------");
	}
	
	public void capnhat_thoigian() throws SQLException {
//		String query1="UPDATE active, history set active.Duration=timediff(history.Cleared_Time,active.Raised_Time)\r\n"
//				+ "WHERE active.NE=history.NE and active.Alarm_Code=history.Alarm_Code and active.Raised_Time=history.Raised_Time AND history.Cleared_Time;";
//		PreparedStatement st=conn.prepareStatement(query1);
//		st.execute();
		String query2="SELECT active.id,active.NE,active.Alarm_Code,active.Raised_Time,history.Cleared_Time\r\n"
				+ "FROM active,history\r\n"
				+ "WHERE active.NE=history.NE and active.Alarm_Code=history.Alarm_Code and active.Raised_Time=history.Raised_Time and history.Cleared_Time IS NOT NULL;";
		String query3="UPDATE active set Duration=? WHERE id=?";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}
		
		conn.setAutoCommit(false); 
		PreparedStatement st=conn.prepareStatement(query3);
		Statement sta= conn.createStatement();
		ResultSet rs=sta.executeQuery(query2);
		
		while(rs.next()) {
			long id= rs.getLong("id");
			String raise=rs.getString("Raised_Time");
			String clear=rs.getString("Cleared_Time");
			
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			 
			LocalDateTime dateTime1= LocalDateTime.parse(raise, formatter);
			LocalDateTime dateTime2= LocalDateTime.parse(clear, formatter);
			
			long diff = java.time.Duration.between(dateTime1, dateTime2).toSeconds();
			int hh=(int) (diff / ( 60 * 60 ));
			int mm=(int) (diff /(60) %60);
			int ss=(int) (diff % 60);
			String dur = "";
			if(hh>9) dur+=String.valueOf(hh)+":"; 
				else dur+="0"+String.valueOf(hh)+":";
			if(mm>9) dur+=String.valueOf(mm)+":"; 
				else dur+="0"+String.valueOf(mm)+":";
			if(ss>9) dur+=String.valueOf(ss); 
				else dur+="0"+String.valueOf(ss);
			st.setString(1, dur);
			st.setLong(2, id);
			st.addBatch();
		}
		st.executeBatch();
		conn.commit();
		rs.close();
		sta.close();
		st.close();
		conn.close();
		System.out.println("cap nhat thoi gian thanh cong");
		System.out.println("---------------------");
	}
	
	public void capnhat_trangthai(List<String[]> lines) throws SQLException {
		String query1="UPDATE active, history set active.Status='Cleared'\r\n"
				+ "WHERE active.NE=? and active.Alarm_Code=? and active.Raised_Time=? AND history.Cleared_Time IS NOT NULL;";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}

		conn.setAutoCommit(false);
		PreparedStatement st=conn.prepareStatement(query1);
		
		String ne = null, alarm_code=null,raised_time = null;
		int dong=0;
		for(String[] line : lines) {
			if(dong>=2) {
				int i=1;
				for(String x:line) {
					if(i==3) ne=x;
					if(i==7) alarm_code=x;
					if(i==9) raised_time=x;
					i++;
				}

				st.setString(1, ne);
				st.setString(2, alarm_code);
				st.setString(3, raised_time);
				st.addBatch();
			}
			dong++;
		}
		
		st.executeBatch();
		conn.commit();
		st.close();
		conn.close();
		System.out.println("cap nhat trang thai thanh cong");
		System.out.println("---------------------");
	}
	
	public void capnhat_thoigain(List<String[]> lines) throws SQLException {
		String query1="UPDATE active, history set active.Duration=?\r\n"
				+ "WHERE active.NE=? and active.Alarm_Code=? and active.Raised_Time=? AND history.Cleared_Time IS NOT NULL;";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}

		conn.setAutoCommit(false);
		PreparedStatement st=conn.prepareStatement(query1);
		
		String ne = null, alarm_code=null,raised_time = null,cleared_time = null;
		int dong=0;
		for(String[] line : lines) {
			if(dong>=2) {
				int i=1;
				for(String x:line) {
					if(i==3) ne=x;
					if(i==7) alarm_code=x;
					if(i==9) raised_time=x;
					if(i==31) cleared_time=x;
					i++;
				}
				if(raised_time==null || cleared_time==null);
				else {
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"); 
					LocalDateTime dateTime1= LocalDateTime.parse(raised_time, formatter);
					LocalDateTime dateTime2= LocalDateTime.parse(cleared_time, formatter);
					
					long diff = java.time.Duration.between(dateTime1, dateTime2).toSeconds();
					int hh=(int) (diff / ( 60 * 60 ));
					int mm=(int) (diff /(60) %60);
					int ss=(int) (diff % 60);
					String dur = "";
					if(hh>9) dur+=String.valueOf(hh)+":"; 
						else dur+="0"+String.valueOf(hh)+":";
					if(mm>9) dur+=String.valueOf(mm)+":"; 
						else dur+="0"+String.valueOf(mm)+":";
					if(ss>9) dur+=String.valueOf(ss); 
						else dur+="0"+String.valueOf(ss);
					
					st.setString(1, dur);
					st.setString(2, ne);
					st.setString(3, alarm_code);
					st.setString(4, raised_time);
					st.addBatch();
				}
			}
			dong++;
		}
		
		st.executeBatch();
		conn.commit();
		st.close();
		conn.close();
		System.out.println("cap nhat thoi gian thanh cong");
		System.out.println("---------------------");
	}
	
	//ham lay alarm code khong trung
	public List<List<String>> get_alarm_code() throws SQLException{
		String query="SELECT Alarm_Code "
				+ "FROM active "
				+ "GROUP BY Alarm_Code";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}
		
		Statement st= conn.createStatement();
		ResultSet rs=st.executeQuery(query);
		
		List<List<String>> listcode= new ArrayList<>();
		while(rs.next()) {
			List<String> list = new ArrayList<>();
			String code=rs.getString("Alarm_Code");
			list.add(code);
			String[] codepart=code.split("[\\(,\\)]");
			for(String x:codepart) {
				list.add(x);
			}
			listcode.add(list);
		}
		
		rs.close();
		st.close();
		conn.close();
		return listcode;
	}
	
	//ham tao view xem level cua alarm code
	public void alarm_code_level() throws SQLException {
		List<List<String>> code= get_alarm_code();
		String query1="UPDATE active,group_cause \r\n"
				+ "SET active.lv=group_cause.level\r\n"
				+ "WHERE \r\n"
				+ "	active.Alarm_Code=? \r\n"
				+ "	and group_cause.cause=?;";
		String query2="UPDATE active,group_cause \r\n"
				+ "SET active.lv=group_cause.level\r\n"
				+ "WHERE \r\n"
				+ "active.lv is null and active.Alarm_Code=? \r\n"
				+ "	and group_cause.cause like ?;";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}
		
		conn.setAutoCommit(false); 
		PreparedStatement st1=conn.prepareStatement(query1);
		PreparedStatement st2=conn.prepareStatement(query2);
		PreparedStatement st3=conn.prepareStatement(query2);

		for(List<String> x:code) {
			st1.setString(1, x.get(0));
			st1.setString(2, x.get(0));
			st1.addBatch();
			
			st2.setString(1, x.get(0));
			st2.setString(2, "%"+x.get(2)+"%");
			st2.addBatch();
			
			st3.setString(1, x.get(0));
			st3.setString(2, "%"+x.get(1)+"%");
			st3.addBatch();
		}
		st1.executeBatch();
		conn.commit();
		
		st2.executeBatch();
		conn.commit();
		
		st3.executeBatch();
		conn.commit();
		
		st1.close();
		st2.close();
		st3.close();
		conn.close();
		System.out.println("cap nhat muc thanh cong ");
		System.out.println("---------------------");
	}

	public void export_code_lv() {
		String query="CREATE or replace view code_lv AS\r\n"
				+ "SELECT * \r\n"
				+ "FROM active, (SELECT level.id as lv_id,level.code,level.name FROM level) as lv_tb\r\n"
				+ "WHERE active.lv= lv_tb.lv_id";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
			Statement st= conn.createStatement();
			st.execute(query);
			st.close();
			conn.close();
			System.out.println("export thanh cong ");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}
		System.out.println("---------------------");
	}
}
