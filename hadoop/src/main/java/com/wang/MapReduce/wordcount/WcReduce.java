package com.wang.MapReduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-07 11:11
 */
public class WcReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        intWritable.set(sum);
        context.write(key,intWritable);
    }
}
