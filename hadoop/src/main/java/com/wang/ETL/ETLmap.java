package com.wang.ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-22 18:11
 */
public class ETLmap extends Mapper<LongWritable, Text,Text, NullWritable> {
     Text text =new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String clear = ETLUtiles.clear(line);
        if (clear == null) {
        }else{
            text.set(clear);
            context.write(text,NullWritable.get());
        }

    }
}
