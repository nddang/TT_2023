package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Iterator;
import org.apache.poi.ss.usermodel.Row;

public class CauseDao {
	public void them(Iterator<Row> iterator) throws SQLException, ParseException {
		String query="INSERT INTO `cause`( `cause`, `level`) VALUES (?,?)";
		
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
		
		Row firstrow=iterator.next();
		while(iterator.hasNext()) {
			Row currentRow = iterator.next();
	        st.setString(1, currentRow.getCell(0).getStringCellValue());
	        st.setInt(2, (int) currentRow.getCell(1).getNumericCellValue());
	        st.addBatch();
		}
		
		st.executeBatch();
		conn.commit();
		st.close();
		conn.close();
		System.out.println("them thanh cong ");
		System.out.println("---------------------");
	}
	
	public void group_cause() {
		String query="CREATE OR REPLACE VIEW group_cause AS "
				+ "SELECT * FROM cause "
				+ "GROUP BY cause "
				+ "ORDER BY id ASC;";
		
		Connection conn=null;
		try {
			conn= Dao.getConnection();
			System.out.println("ket noi thanh cong");
			Statement st= conn.createStatement();
			st.execute(query);
			st.close();
			conn.close();
			System.out.println("query thanh cong ");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Connection to database failed");
		}
		System.out.println("---------------------");
	}
	
}
