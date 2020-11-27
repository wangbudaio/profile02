package com.wang.join.Counter;

import com.wang.join.mapreduce.Mapjoin;
import com.wang.join.mapreduce.Mapjoindriver;
import com.wang.join.mapreduce.reduceBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @author 王继昌
 * @create 2020-09-10 14:18
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);

        job.setJarByClass(Driver.class);

        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("d:/input4/"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output4"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
