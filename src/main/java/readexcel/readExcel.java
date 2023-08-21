package readexcel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import dao.CauseDao;
import dao.LevelDao;


public class readExcel {

	public static void main(String[] args) throws SQLException, ParseException {
		File f=new File("data/exportAlarm.xlsx");
	    try {
	    	FileInputStream excelFile = new FileInputStream(f);
			Workbook workbook = new XSSFWorkbook(excelFile);
			Sheet datatypeSheet1 = workbook.getSheetAt(0);
			Sheet datatypeSheet2 = workbook.getSheetAt(1);
		    
		    Iterator<Row> iterator1 = datatypeSheet1.iterator();
		    CauseDao causedao= new CauseDao();
		    causedao.them(iterator1);
		    
		    Iterator<Row> iterator2 = datatypeSheet2.iterator();
		    LevelDao leveldao= new LevelDao();
		    leveldao.them(iterator2);
		    
		    workbook.close();
		    excelFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
