import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by liuxun on 2017/8/10.
 */
public class MainTest {

    public static void main(String[] args) {
//        int a=15;
//        int b=15;
//        System.out.println("a 与 b 异或的结果是："+(a^b));
//        System.out.println(1 ^ 3);
//        System.out.println(Integer.toBinaryString(15));
//        System.out.println(Integer.toBinaryString(10));
//        System.out.println(Integer.toBinaryString(a^b));
//        System.out.println(Integer.parseInt(Integer.toBinaryString(10),2));
//        System.out.println(hash("liuxun"));

//        StringBuffer x=new StringBuffer("A");
//        StringBuffer y=new StringBuffer("B");
//        operate(x,y);
//        System.out.println(x + "," + y);


    }
    static final int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    static void operate(StringBuffer x,StringBuffer y){
        x.append(y);
        y=x;
    }

    class HelloA{

        public HelloA(){

        }
        {

            System.out.println("A class");
        }

    }
}
