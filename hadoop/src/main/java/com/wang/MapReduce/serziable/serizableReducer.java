package com.wang.MapReduce.serziable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-07 15:06
 */
public class serizableReducer  extends Reducer<Text,Flowwriter,Text,Flowwriter> {
    Flowwriter flowwriter = new Flowwriter();
    @Override
    protected void reduce(Text key, Iterable<Flowwriter> values, Context context) throws IOException, InterruptedException {
        Long upFlow = 0L;
        Long downFlow = 0L;
        for (Flowwriter value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        flowwriter.set(upFlow,downFlow);
        context.write(key,flowwriter);
    }
}
