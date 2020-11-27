package com.wang.MapReduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-07 11:09
 */
public class WcMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text text = new Text();
    private IntWritable intWritable = new  IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] s = value.toString().split(" ");
        for (String s1 : s) {
            this.text.set(s1);
            context.write(text,intWritable);
        }
    }
}
