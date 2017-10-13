package hive;

import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by liuxun on 2017/8/14.
 */
public class HiveTestOne {
    static Logger logger = Logger.getLogger(HiveTestOne.class);
    public static void main(String[] args) {
        Connection conn = HiveService.getConn();
        Statement stmt = null;
        try {
            stmt = HiveService.getStmt(conn);
        } catch (SQLException e) {
            logger.debug("1");
        }
//        #插入数据
        try {
            String filepath = "/export/servers/data/userinfo.txt";
            String sql = "load data local inpath '" + filepath + "' into table usr_chain partition(city='beijing')";
            System.out.println("Running:" + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        #查询数据

        String sql = "select * from usr_chain";

        ResultSet res = null;
        try {
            res = stmt.executeQuery(sql);

            ResultSetMetaData meta = res.getMetaData();

            for(int i = 1; i <= meta.getColumnCount(); i++){
                System.out.print(meta.getColumnName(i) + "    ");
            }
            System.out.println();
            while(res.next()){
                System.out.print(res.getString(1) + "    ");
                System.out.print(res.getString(2) + "    ");
                System.out.print(res.getString(3) + "    ");
                System.out.print(res.getString(4) + "    ");
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }



        HiveService.closeStmt(stmt);
        HiveService.closeConn(conn);
    }
}
