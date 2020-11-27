package com.wang.MapReduce.serziable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;


import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-07 15:05
 */
public class serizableMapper  extends Mapper<LongWritable, Text,Text,Flowwriter> {
    Text k = new Text();
    Flowwriter v = new Flowwriter();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        k.set(split[1]);
        Long aLong = Long.valueOf(split[split.length - 2]);
        Long aLong1 = Long.valueOf(split[split.length - 3]);
        v.set(aLong,aLong1);
        context.write(k,v);
    }
}
