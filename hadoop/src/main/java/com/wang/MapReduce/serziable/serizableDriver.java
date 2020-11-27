package com.wang.MapReduce.serziable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


/**
 * @author 王继昌
 * @create 2020-09-07 15:06
 */
public class serizableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration entries = new Configuration();
        Job instance = Job.getInstance(entries);

        instance.setJarByClass(serizableDriver.class);

        instance.setMapperClass(serizableMapper.class);
        instance.setReducerClass(serizableReducer.class);


        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(Flowwriter.class);

        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(Flowwriter.class);

        FileInputFormat.addInputPath(instance,new Path("D:\\input"));
        FileOutputFormat.setOutputPath(instance,new Path("D:\\input1"));

        boolean b = instance.waitForCompletion(true);
        System.exit(b? 0 : 1);
    }
}
