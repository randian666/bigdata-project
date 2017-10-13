package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
/**
 * jdbc连接Hive
 * Created by liuxun on 2017/8/14.
 */
public class HiveService {
    static Logger logger = Logger.getLogger(HiveService.class);
    //hive的jdbc驱动类
    public static String dirverName = "org.apache.hive.jdbc.HiveDriver";
    //连接hive的URL hive1.2.1版本需要的是jdbc:hive2，而不是 jdbc:hive
    public static String url = "jdbc:hive2://192.168.38.135:10000/hive";
    //登录linux的用户名  一般会给权限大一点的用户，否则无法进行事务形操作
    public static String user = "hadoop";
    //登录linux的密码
    public static String pass = "liuxun";
    /**
     * 创建连接
     * @return
     * @throws SQLException
     */
    public static Connection getConn(){
        Connection conn = null;
        try {
            Class.forName(dirverName);
            conn = DriverManager.getConnection(url, user, pass);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 创建命令
     * @param conn
     * @return
     * @throws SQLException
     */
    public static Statement getStmt(Connection conn) throws SQLException{
        logger.debug(conn);
        if(conn == null){
            logger.debug("this conn is null");
        }
        return conn.createStatement();
    }

    /**
     * 关闭连接
     * @param conn
     */
    public static void closeConn(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 关闭命令
     * @param stmt
     */
    public static void closeStmt(Statement stmt){
        try {
            stmt.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

//    private static void countData(Statement stmt, String tableName)
//            throws SQLException {
//        sql = "select count(1) from " + tableName;
//        System.out.println("Running:" + sql);
//        res = stmt.executeQuery(sql);
//        System.out.println("执行“regular hive query”运行结果:");
//        while (res.next()) {
//            System.out.println("count ------>" + res.getString(1));
//        }
//    }
//
//    private static void selectData(Statement stmt, String tableName)
//            throws SQLException {
//        sql = "select * from " + tableName;
//        System.out.println("Running:" + sql);
//        res = stmt.executeQuery(sql);
//        System.out.println("执行 select * query 运行结果:");
//        while (res.next()) {
//            System.out.println(res.getInt(1) + "\t" + res.getString(2));
//        }
//    }
//
//    private static void loadData(Statement stmt, String tableName)
//            throws SQLException {
//        //目录 ，我的是hive安装的机子的虚拟机的home目录下
//        String filepath = "user.txt";
//        sql = "load data local inpath '" + filepath + "' into table "
//                + tableName;
//        System.out.println("Running:" + sql);
//        stmt.execute(sql);
//    }
//
//    private static void describeTables(Statement stmt, String tableName)
//            throws SQLException {
//        sql = "describe " + tableName;
//        System.out.println("Running:" + sql);
//        res = stmt.executeQuery(sql);
//        System.out.println("执行 describe table 运行结果:");
//        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2));
//        }
//    }
//
//    private static void showTables(Statement stmt, String tableName)
//            throws SQLException {
//        sql = "show tables '" + tableName + "'";
//        System.out.println("Running:" + sql);
//        res = stmt.executeQuery(sql);
//        System.out.println("执行 show tables 运行结果:");
//        if (res.next()) {
//            System.out.println(res.getString(1));
//        }
//    }
//
//    private static void createTable(Statement stmt, String tableName)
//            throws SQLException {
//        sql = "create table "
//                + tableName
//                + " (key int, value string)  row format delimited fields terminated by '\t'";
//        stmt.execute(sql);
//    }
//
//    private static String dropTable(Statement stmt) throws SQLException {
//        // 创建的表名
//        String tableName = "testHive";
//        sql = "drop table  " + tableName;
//        stmt.execute(sql);
//        return tableName;
//    }
}
