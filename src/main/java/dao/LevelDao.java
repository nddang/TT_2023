package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Row;

public class LevelDao {
	public void them(Iterator<Row> iterator) throws SQLException, ParseException {
		String query="INSERT INTO `level`( `id`, `code`, `name`) VALUES (?,?,?)";
		
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
			st.setInt(1, (int) currentRow.getCell(0).getNumericCellValue());
	        st.setString(2, currentRow.getCell(1).getStringCellValue());
	        st.setString(3, currentRow.getCell(2).getStringCellValue());
	        st.addBatch();
		}
		
		st.executeBatch();
		conn.commit();
		st.close();
		conn.close();
		System.out.println("them thanh cong ");
		System.out.println("---------------------");
	}
}
