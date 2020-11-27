package com.wang.join.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author 王继昌
 * @create 2020-09-10 11:09
 */
public class mycomparator extends WritableComparator {
    public mycomparator() {
        super(reduceBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        reduceBean a1 = (reduceBean) a;
        reduceBean b1 = (reduceBean) b;
        return a1.getPid().compareTo(b1.getPid());
    }
}
