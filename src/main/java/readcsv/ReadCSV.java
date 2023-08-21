package readcsv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;

import dao.ActiveDao;
import dao.HistoryDao;

import com.opencsv.CSVReader;

public class ReadCSV {

	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException, ParseException {
		File dir=new File("data");
		File[] listFile= dir.listFiles();
		for(File f: listFile) {
			String csvFile = f.getCanonicalPath();
	        try (Reader reader = new FileReader(csvFile); CSVReader csvReader = new CSVReader(reader);) {
	        	List<String[]> lines=csvReader.readAll();
	        	lines.remove(lines.size()-1);
	        	String[] fline=lines.get(0);
	        	String table=fline[0];
	        	System.out.println("---------------------");
	        	System.out.println("them vao bang "+table);
	        	ActiveDao actdao=new ActiveDao();
	        	HistoryDao hisdao=new HistoryDao();
	        	if(table.equals("Active Alarm")) actdao.them(lines);
	        	else if(table.equals("History Alarm")) hisdao.them(lines);
	        	csvReader.close();
	        	reader.close();
	        }
		}
		
	}
}
