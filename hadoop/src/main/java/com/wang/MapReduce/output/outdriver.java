package com.wang.MapReduce.output;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-09 19:17
 */
public class outdriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(outdriver.class);

        job.setMapperClass(outMapper.class);
        job.setReducerClass(outreduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("d:/input2/"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output"));
        job.setOutputFormatClass(myOutputFormat.class);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
