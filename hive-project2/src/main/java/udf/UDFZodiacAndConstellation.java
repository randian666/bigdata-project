package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.util.Calendar;

/**
 * 统计显示生肖和星座
 * 输入日期显示字符串. 如果是0就显示生肖,如果是1就显示星座.
 * Created by liuxun on 2017/8/15.
 */
public class UDFZodiacAndConstellation extends UDF {
    public final String[] zodiacArr = { "猴", "鸡", "狗", "猪", "鼠", "牛", "虎", "兔","龙", "蛇", "马", "羊" };
    public final String[] constellationArr = { "水瓶座", "双鱼座", "白羊座", "金牛座","双子座", "巨蟹座", "狮子座", "处女座","天秤座", "天蝎座", "射手座", "魔羯座" };
    public static final int[] constellationEdgeDay = { 20, 19, 21, 21, 21, 22, 23, 23, 23, 23, 22, 22 };

    public Text evaluate(Date date,int type){
        // 这个地方Date的类型是java.sql.Date,和数据库打交道的都是这个.
        // 还可以用DateWritable,这个类面封装了一个java.sql.Date类型.
        java.util.Date udate = new java.util.Date(date.getTime());
        if (type==0){
            return new Text(getZodica(udate));
        }else if(type==1){
            return new Text(getConstellation(udate));
        }else{
            return new Text("NULL");
        }
    }

    /**
     * 根据日期获取生肖
    */
    public String getZodica(java.util.Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return zodiacArr[cal.get(Calendar.YEAR) % 12];
    }
    /**
     * 根据日期获取星座
     * @return
     */
    public String getConstellation(java.util.Date date) {
        if (date == null) {
            return "";
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        if (day < constellationEdgeDay[month]) {
            month = month - 1;
        }
        if (month >= 0) {
            return constellationArr[month];
        }
        // default to return 魔羯
        return constellationArr[11];
    }

    public static void main(String[] args) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new java.util.Date());
        System.out.println(cal.get(Calendar.MONTH)+":"+Calendar.DAY_OF_MONTH);
    }
}
