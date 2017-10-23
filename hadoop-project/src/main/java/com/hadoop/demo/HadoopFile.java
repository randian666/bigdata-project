package com.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;

/**
 * hdfs文件操作
 *windows从hdfs中下载文件需要把hadoop.dll放入c:\windows\system32目录中
 * Created by liuxun on 2017/6/28.
 */
public class HadoopFile {
    private Configuration conf=null;

    public HadoopFile(){
        conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.38.135:9000");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public static void main(String[] args) {
        HadoopFile hadoopFile=new HadoopFile();
        hadoopFile.downloadFile("/export/servers/data/usercount.txt","D:\\");
//        hadoopFile.delFile("/user/hadoop/input/job.txt");
//        hadoopFile.sendFile("/user/hadoop/input","E:\\hadoopdata\\job.txt");
//        hadoopFile.sendFile("/user/hadoop/input","E:\\hadoopdata\\user.txt");
    }
    public boolean sendFile(String path,String localfile){
        File file=new File(localfile);
        if (!file.isFile()) {
            System.out.println(file.getName());
            return false;
        }
        try {
            FileSystem localFS = FileSystem.getLocal(conf);
            FileSystem hadoopFS =FileSystem.get(conf);
            Path hadPath=new Path(path);

            FSDataOutputStream fsOut=hadoopFS.create(new Path(path+"/"+file.getName()));
            FSDataInputStream fsIn=localFS.open(new Path(localfile));
            byte[] buf =new byte[1024];
            int readbytes=0;
            while ((readbytes=fsIn.read(buf))>0){
                fsOut.write(buf,0,readbytes);
            }
            fsIn.close();
            fsOut.close();

            FileStatus[] hadfiles= hadoopFS.listStatus(hadPath);
            for(FileStatus fs :hadfiles){
                System.out.println(fs.toString());
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean delFile(String hadfile){
        try {

            FileSystem hadoopFS =FileSystem.get(conf);
            Path hadPath=new Path(hadfile);
            Path p=hadPath.getParent();
            boolean rtnval= hadoopFS.delete(hadPath, true);

            FileStatus[] hadfiles= hadoopFS.listStatus(p);
            for(FileStatus fs :hadfiles){
                System.out.println(fs.toString());
            }
            return rtnval;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    public boolean downloadFile(String hadfile,String localPath){

        try {
            FileSystem localFS =FileSystem.getLocal(conf);
            FileSystem hadoopFS =FileSystem.get(conf);
            Path hadPath=new Path(hadfile);

            FSDataOutputStream fsOut=localFS.create(new Path(localPath+"/"+hadPath.getName()));
            FSDataInputStream fsIn=hadoopFS.open(hadPath);
            byte[] buf =new byte[1024];
            int readbytes=0;
            while ((readbytes=fsIn.read(buf))>0){
                fsOut.write(buf,0,readbytes);
            }
            fsIn.close();
            fsOut.close();

            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
