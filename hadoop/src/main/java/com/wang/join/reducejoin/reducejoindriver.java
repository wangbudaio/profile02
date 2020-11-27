package com.wang.join.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-10 11:40
 */
public class reducejoindriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(reducejoindriver.class);

        job.setMapperClass(mapjoin.class);
        job.setReducerClass(reducejoins.class);

        job.setMapOutputKeyClass(reduceBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(reduceBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.addInputPath(job,new Path("d:/input3/"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
