package com.wang.join.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author 王继昌
 * @create 2020-09-10 11:07
 */
public class reducejoins extends Reducer<reduceBean, NullWritable,reduceBean,NullWritable> {
    @Override
    protected void reduce(reduceBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getPname();
        while (iterator.hasNext()){
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }
    }
}
