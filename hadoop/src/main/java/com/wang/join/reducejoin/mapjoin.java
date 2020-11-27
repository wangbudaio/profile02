package com.wang.join.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-10 10:55
 */
public class mapjoin extends Mapper<LongWritable, Text,reduceBean, NullWritable> {
    private String name;
    private reduceBean rb = new reduceBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) split;
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if (name.contains("order")){
            rb.setId(split[0]);
            rb.setPid(split[1]);
            rb.setAmount(Integer.valueOf(split[2]));
            rb.setPname("");

        }else{
            rb.setId("");
            rb.setPid(split[0]);
            rb.setAmount(0);
            rb.setPname(split[1]);
        }
        context.write(rb,NullWritable.get());

    }
}
