package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 *  HIVE自定义函数，需要继承UDF类并实现evaluate函数。
 *  在查询执行过程中，查询中对应的每个应用到这个函数的地方都会对这个类进行实例化。
 *  对于每行输入都会调用到evaluate函数。而evaluate()处理后的值会返回给Hive。
 *  同时用户是可以重载evaluate方法的。hive会像java的方法重载一样，自动选择匹配的方法。
 * Created by liuxun on 2017/8/15.
 */
public class ConcatString extends UDF{

    public Text evaluate(Text a,Text b){
        return new Text(a.toString()+"#####"+b.toString());
    }
}

