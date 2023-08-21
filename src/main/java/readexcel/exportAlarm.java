package readexcel;

import java.sql.SQLException;

import dao.ActiveDao;
import dao.CauseDao;

public class exportAlarm {

	public static void main(String[] args) throws SQLException {
		CauseDao causedao= new CauseDao();
		causedao.group_cause();
		ActiveDao act=new ActiveDao();
		act.alarm_code_level();;
		act.export_code_lv();
		
	}
}
