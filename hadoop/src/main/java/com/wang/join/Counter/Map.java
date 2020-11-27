package com.wang.join.Counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-10 14:18
 */
public class Map extends Mapper<LongWritable, Text,Text, NullWritable> {
    private Counter start;
    private Counter stop;
    private Counter all;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        start = context.getCounter("ETL","start-sum");
        stop = context.getCounter("ETL","stop-sum");
        all = context.getCounter("ETL","all-sum");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        all.increment(1);
        String[] s = value.toString().split(" ");
        if(s.length > 11){
            context.write(value,NullWritable.get());
            start.increment(1);
        }else {
            context.write(value,NullWritable.get());
            stop.increment(1);
        }
    }
}
