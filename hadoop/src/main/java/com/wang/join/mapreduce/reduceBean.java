package com.wang.join.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-10 10:44
 */
public class reduceBean implements WritableComparable<reduceBean> {
    private String id;
    private String pid;
    private int amount;
    private String pname;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
        return id + "\t" + amount +  "\t" +pname;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
    }

    @Override
    public int compareTo(reduceBean o) {
        int i = this.pid.compareTo(o.pid);
        if (i == 0) {
            return -this.pname.compareTo(o.pname);
        }
        return i;
    }
}
