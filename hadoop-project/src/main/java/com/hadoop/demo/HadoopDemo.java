package com.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * hadoop java api基础操作
 * Created by liuxun on 2017/6/28.
 */
public class HadoopDemo {
    public static void main(String[] args) {
//        write();
//        exists();
        reader();
    }
    //写入文件
    private static void write(){
        try {
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS","hdfs://192.168.38.135:9000");
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            byte[] buff = "Hello world My Name is LiuXun".getBytes(); // 要写入的内容
            String filename = "count.txt"; //要写入的文件名
            FSDataOutputStream os = fs.create(new Path(filename));
            os.write(buff,0,buff.length);
            System.out.println("Create:"+ filename);
            os.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //判断文件是否存在
    private static void exists(){
        try {
            String filename = "test";
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://192.168.38.135:9000");
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(filename))){
                System.out.println("文件存在");
            }else{
                System.out.println("文件不存在");
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //读取文件
    private static void reader(){
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://192.168.38.135:9000");
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path("test");
            FSDataInputStream getIt = fs.open(file);
            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            String content = d.readLine(); //读取文件一行
            System.out.println(content);
            d.close(); //关闭文件
            fs.close(); //关闭hdfs
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
