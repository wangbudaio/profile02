package com.wang.MapReduce.serziable;

import org.apache.hadoop.io.Writable;

import javax.ws.rs.GET;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-07 15:06
 */
public class Flowwriter implements Writable {
    private Long upFlow;
    private Long downFlow;
    private Long Flow;


    public void set(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.Flow = getDownFlow()+getUpFlow();
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getFlow() {
        return Flow;
    }

    public void setFlow(Long flow) {
        Flow = flow;
    }


    //3  写序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
            out.writeLong(upFlow);
            out.writeLong(downFlow);
            out.writeLong(Flow);
    }

    //4  反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
       this.upFlow = in.readLong();
       this.downFlow = in.readLong();
       this.Flow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + Flow;
    }
}
