package com.wang.MapReduce.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;



import java.io.IOException;



/**
 * @author 王继昌
 * @create 2020-09-07 11:12
 */
public class driver{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //生成job对象
        Configuration configuration = new Configuration();
//******************************************************
        //设置HDFS NameNode的地址
        configuration.set("fs.defaultFS", "hdfs://hadoop102:9820");
        // 指定MapReduce运行在Yarn上
        configuration.set("mapreduce.framework.name","yarn");
        // 指定mapreduce可以在远程集群运行
        configuration.set("mapreduce.app-submission.cross-platform","true");
        //指定Yarn resourcemanager的位置
        configuration.set("yarn.resourcemanager.hostname","hadoop103");
//******************************************************


        Job job = Job.getInstance(configuration);
        //设置jar包位置
//        job.setJarByClass(driver.class);
        job.setJar("G:\\idea\\works\\profile02\\hadoop\\target\\hadoop-1.0-SNAPSHOT.jar");
        //设置我们写的map和reduce
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);
        //设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
