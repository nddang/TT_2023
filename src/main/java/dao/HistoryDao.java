package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class HistoryDao {
	public void them(List<String[]> lines) throws SQLException, ParseException {
		String query="INSERT INTO `history`(`Perceived_Severity`, `Alarm_Type`, `NE`, `Location`, "
				+ "`Additional_NE`, `Additional_Location`, `Alarm_Code`, `Specific_Problem`, `Raised_Time`, "
				+ "`Changed_Time`, `NE_Type`, `Remark`, `ACK_State`, `(Un)ACK_Time`, `(Un)ACK_User_ID`, "
				+ "`(Un)ACK_System_ID`, `Comment_Time`, `Comment`, `Comment_User_ID`, `Comment_System_ID`, "
				+ "`Product`, `Alarm_Aid`, `Alarm_ID`, `NE_IP`, `Link`, `NE_Group`, `NE_Agent`, "
				+ "`System_Type`, `Additional_Information`, `Duration`, `Cleared_Time`, `Cleared_User_ID`, "
				+ "`Cleared_System_ID`, `Path_Name`, `Cleared_Type`, `Custom_Attribute_1`, "
				+ "`Custom_Attribute_2`, `Custom_Attribute_3`, `Custom_Attribute_4`, `Custom_Attribute_5`, "
				+ "`Custom_Attribute_6`, `Custom_Attribute_7`, `Custom_Attribute_8`, `Custom_Attribute_9`, "
				+ "`Custom_Attribute_10`)"
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi DB thanh cong");
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
				for(String x:line) {
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
					    	case 31:
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
		
}
