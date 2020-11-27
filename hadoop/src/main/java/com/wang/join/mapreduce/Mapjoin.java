package com.wang.join.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;


import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 王继昌
 * @create 2020-09-10 13:09
 */
public class Mapjoin extends Mapper<LongWritable, Text,reduceBean, NullWritable> {
    private Map<String,String> map = new HashMap<>();
    private reduceBean rb = new reduceBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fs = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader in = new BufferedReader(new InputStreamReader((InputStream)fs));
        String line;
        while (StringUtils.isNotEmpty(line = in.readLine())){
            String[] split = line.split("\t");
            map.put(split[0],split[1]);
        }
        IOUtils.closeStream(in);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        rb.setId(split[0]);
        rb.setPid(split[1]);
        rb.setAmount(Integer.valueOf(split[2]));
        rb.setPname(map.get(split[1]));

        context.write(rb,NullWritable.get());
    }
}
