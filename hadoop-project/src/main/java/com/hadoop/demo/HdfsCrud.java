package com.hadoop.demo;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
/**
 * Created by liuxun on 2017/10/13.
 */
public class HdfsCrud {

    //文件系统连接到 hdfs的配置信息
    private static Configuration getConf() {
        // 创建配置实例
        Configuration conf = new Configuration();
        // 这句话很关键，这些信息就是hadoop配置文件中的信息
        conf.set("fs.defaultFS", "hdfs://192.168.38.135:9000");
        return conf;
    }

    /*
     * 获取HDFS集群上所有节点名称信息
     */
    public static void getDateNodeHost() throws IOException {
        // 获取连接配置实例
        Configuration conf = getConf();
        // 创建文件系统实例
        FileSystem fs = FileSystem.get(conf);
        // 强转为分布式文件系统hdfs
        DistributedFileSystem hdfs = (DistributedFileSystem)fs;
        // 获取分布式文件系统hdfs的DataNode节点信息
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        // 遍历输出
        for(int i=0;i<dataNodeStats.length;i++){
            System.out.println("DataNode_"+i+"_Name:"+dataNodeStats[i].getHostName());
        }
        // 关闭连接
        hdfs.close();
        fs.close();
    }

    /*
     * upload the local file to the hds
     * 路径是全路径
     */
    public static void uploadLocalFile2HDFS(String s, String d)  throws IOException {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        // 创建路径实例
        Path src = new Path(s);
        Path dst = new Path(d);
        // 拷贝文件
        fs.copyFromLocalFile(src, dst);
        // 关闭连接
        fs.close();
    }

    /*
     * create a new file in the hdfs.
     * notice that the toCreateFilePath is the full path
     * and write the content to the hdfs file.
     */
    public static void createNewHDFSFile(String toCreateFilePath, String content) throws IOException {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 创建输出流实例
        FSDataOutputStream os = fs.create(new Path(toCreateFilePath));
        // 写入UTF-8格式字节数据
        os.write(content.getBytes("UTF-8"));

        // 关闭连接
        os.close();
        fs.close();
    }

    /*
     * 复制本地文件到HDFS（性能与缓存大小有关，越大越好，可设为128M）
     * notice that the toCreateFilePath is the full path
     * and write the content to the hdfs file.
     */
    public static void copytoHDFSFile(String toCreateFilePath, String localFilePath) throws IOException {
        // 读取本地文件
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(localFilePath));

        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        // 创建HDFS输出流实例
        FSDataOutputStream os = fs.create(new Path(toCreateFilePath));

        // 两种方式其中的一种一次读写一个字节数组
        byte[] bys = new byte[128000000];
        int len = 0;
        while ((len = bis.read(bys)) != -1) {
            os.write(bys, 0, len);
            os.hflush();
        }

        // 关闭连接
        os.close();
        fs.close();
    }


    /*
     * read the hdfs file content
     * notice that the dst is the full path name
     * 读取文件，返回buffer【需要再print】
     */
    public static byte[] readHDFSFile(String filename) throws Exception  {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 创建路径实例
        Path readPath = new Path(filename);

        //  检查文件是否存在
        if (fs.exists(readPath))  {
            FSDataInputStream is = fs.open(readPath);
            // 获取文件信息，以便确定buffer大小
            FileStatus stat = fs.getFileStatus(readPath);

            // 创建buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];

            // 读取全部数据，存入buffer
            is.readFully(0, buffer);

            // 关闭连接
            is.close();
            fs.close();

            // 返回读取到的数据
            return buffer;
        }else{
            throw new Exception("the file is not found .");
        }
    }
    /*
     * 直接读取、打印文件
     */
    public static void read(String fileName)throws Exception {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 创建路径实例
        Path readPath = new Path(fileName);

        // 读取数据，打开流文件
        FSDataInputStream inStream = fs.open(readPath);

        try{
            // 读取流文件，打印，缓存4096，操作后不用关闭
            IOUtils.copyBytes(inStream, System.out, 4096, false);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            // close steam
            IOUtils.closeStream(inStream);
        }
    }

    /*
     * delete the hdfs file
     * notice that the dst is the full path name
     * 删除HDFS文件
     */
    public static boolean deleteHDFSFile(String dst) throws IOException {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 创建路径实例
        Path path = new Path(dst);

        // 删除文件，并返回是否成功
        @SuppressWarnings("deprecation")
        boolean isDeleted = fs.delete(path);

        // 关闭文件连接
        fs.close();

        // 返回操作结果
        return isDeleted;
    }

    /*
     * make a new dir in the hdfs
     * the dir may like '/tmp/testdir'
     * 创建目录
     */
    public static void mkdir(String dir) throws IOException  {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 创建路径
        fs.mkdirs(new Path(dir));

        // 关闭文件连接
        fs.close();
    }

    /*
     * delete a dir in the hdfs
     * dir may like '/tmp/testdir'
     * 删除目录
     */
    @SuppressWarnings("deprecation")
    public static void deleteDir(String dir) throws IOException  {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 删除目录
        fs.delete(new Path(dir));

        // 关闭文件连接
        fs.close();
    }

    /**
     * @Title: listAll
     * @Description: 列出目录下所有文件
     * @return void    返回类型
     * @throws
     */
    @SuppressWarnings("deprecation")
    public static void listAll(String dir) throws IOException {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // 获取目录列表
        FileStatus[] stats = fs.listStatus(new Path(dir));
        // 遍历打印
        for(int i = 0; i < stats.length; ++i) {
            if (!stats[i].isDir()){
                // regular file
                System.out.println(stats[i].getPath().toString());
            }else{
                // dir
                System.out.println(stats[i].getPath().toString());
            }
        }

        // 关闭文件连接
        fs.close();
    }

    public static void main(String[] args) throws Exception {

        //getDateNodeHost();

        //uploadLocalFile2HDFS("E:/1.txt","/tmp/1.txt");//E盘下文件传到hdfs上

        //createNewHDFSFile("/tmp/create2", "hello");
        copytoHDFSFile("/tmp/create2", "C://user_visit_action.txt");
        //System.out.println(new String(readHDFSFile("/tmp/create2")));
        //readHDFSFile("/tmp/create2");
        //deleteHDFSFile("/tmp/create2");

        //mkdir("/tmp/testdir");
        //deleteDir("/tmp/testdir");
        listAll("/tmp/");
    }

}