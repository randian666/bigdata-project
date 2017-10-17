package com.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 查找最高气温demo
 * IntWritable 类型相当于Java中的Integer类型
 * Created by liuxun on 2017/7/19.
 */
public class MaxTemperature{
    private final char SYMBOL_ADD='+';
    /**
     * 查找最高气温的Mapper
     * Mapper是一个泛型类型，它有四个参数类型，分别指定map函数的输入键、输入值、输出键和输出值的类型。
     * 本例输入键是一个长整数偏移量，输入值是一行文本，输入键是年份、输出值是气温整数
     * LongWritable 相当于Java的Long类型
     * Text 类型相当于Java中的String类型
     * IntWritable 类型相当于Java中的Integer类型
     */
    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line=value.toString();
                String year=line.substring(15,19);
                int airTemperature;
                if (line.charAt(87)==SYMBOL_ADD){
                    airTemperature=Integer.parseInt(line.substring(88,92));//获取气温
                }else{
                    airTemperature=Integer.parseInt(line.substring(87,92));//获取气温
                }
                String quality=line.substring(92,93);
//                System.out.println("year:"+year+"------airTemperature:"+airTemperature+"-----quality:"+quality);
                if (airTemperature!=9999 && quality.matches("[01459]")){//过滤
                    context.write(new Text(year),new IntWritable(airTemperature));
                }
        }
    }
    /**
     * 查找最高气温的Reducer
     * Mapper是一个泛型类型，它有四个参数类型用于指定输入、输出   类型。reduce函数的输入类型必须匹配map函数的输出类型
     */
    public static class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (null==values){
                return;
            }
            int maxValue=Integer.MIN_VALUE;
            for (IntWritable v:values){
                maxValue=Math.max(maxValue,v.get());//找出最大气温
            }
            context.write(key,new IntWritable(maxValue));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length!=2){
            System.out.println("Usage:MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Job job=Job.getInstance(conf,"MaxTemperature");
        //Hadoop利用这个类查找包含它的JAR文件
        job.setJarByClass(MaxTemperature.class);
        //指定map类型和reduce类型
        job.setMapperClass(MaxTemperatureMapper.class);
//        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        //输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //获取输入参数，设置输入文件目录和输出文件目录
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        //提交 job，等待运行结果，并在客户端显示运行信息，最后结束程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
