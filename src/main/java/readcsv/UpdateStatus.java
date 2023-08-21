package readcsv;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;

import com.opencsv.CSVReader;

import dao.ActiveDao;

public class UpdateStatus {

	public static void main(String[] args) throws SQLException, ParseException, IOException {
//		cap nhat toan bo
//		ActiveDao.capnhat_trangthai();
//		ActiveDao.capnhat_thoigian();
		
//		cap nhat theo tung file history
		String csvFile = "data/Northbound FM History Alarms_20230704001331728_20230704001831756_1_1.csv";
        try (Reader reader = new FileReader(csvFile); CSVReader csvReader = new CSVReader(reader);) {
        	List<String[]> lines=csvReader.readAll();
        	lines.remove(lines.size()-1);
        	ActiveDao actdao=new ActiveDao();
        	actdao.capnhat_trangthai(lines);
        	actdao.capnhat_thoigain(lines);
        	csvReader.close();
        	reader.close();
        }

	}
}
