package com.wang.join.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;


/**
 * @author 王继昌
 * @create 2020-09-10 13:13
 */
public class Mapjoindriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);

        job.setJarByClass(Mapjoindriver.class);

        job.setMapperClass(Mapjoin.class);
        job.setNumReduceTasks(0);

        job.addCacheFile(URI.create("file:///d:/input3/pd.txt"));

        job.setMapOutputKeyClass(reduceBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("d:/input3/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output3"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
