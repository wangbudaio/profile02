package com.wang.MapReduce.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-09 18:33
 */
public class myRecordwrite extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguigu;
    FSDataOutputStream other;

    public myRecordwrite(TaskAttemptContext job) throws IOException {

        Configuration configuration = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        String dir = configuration.get(FileOutputFormat.OUTDIR);
        atguigu = fileSystem.create(new Path(dir+"/atguigu"));
        other = fileSystem.create(new Path(dir+"/other"));
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String s = key.toString();
        if (s.contains("atguigu")) {
            atguigu.writeUTF(s+"\n");
        }else{
            other.writeUTF(s+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(atguigu,other);
    }

}
