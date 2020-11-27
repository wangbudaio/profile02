package com.utils.hdfs;

import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;

/**
 * @author 王继昌
 * @create 2020-09-04 20:45
 */
public class utils {


    @Test
    public void test(){
        Boolean aBoolean = utils.get(new Path("/test_complex.bin"),new Path("D:/test_complex.bin"));
        System.out.println(aBoolean);
    }

    //path 是文件地址
    //path1 是上传后的文件地址
    public static Boolean put(Path path, Path path1){

        FileSystem fs = connection.getFs();
        try {
            fs.copyFromLocalFile(path,path1);
        } catch (IOException e) {
            return false;
        }
        connection.close(fs);
        return true;
    }
    //path 是需要下载的文件路径
    //path1 是文件下载的路径
    public static Boolean get(Path path, Path path1){

        FileSystem fs = connection.getFs();
        try {
            fs.copyToLocalFile(false,path,path1,true);
        } catch (IOException e) {
            return false;
        }
        connection.close(fs);
        return true;
    }

    //path 是需要删除的文件路径
    public static Boolean rm(Path path){

        FileSystem fs = connection.getFs();
        try {
            fs.deleteOnExit(path);
        } catch (IOException e) {
            return false;
        }
        connection.close(fs);
        return true;
    }

    //修改文件名字
    //修改前的名字：path
    //修改后的名字：path1
    public static Boolean rename(Path path,Path path1){

        FileSystem fs = connection.getFs();
        try {
            fs.deleteOnExit(path);
        } catch (IOException e) {
            return false;
        }
        connection.close(fs);
        return true;
    }

    //查看路径下的文件
    //查看路径path
    public static RemoteIterator<LocatedFileStatus> ListFiles(Path path){
        FileSystem fs = connection.getFs();
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = null;
        try {
            locatedFileStatusRemoteIterator = fs.listFiles(path,true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        connection.close(fs);
        return locatedFileStatusRemoteIterator;
    }

    //查看路径的是文件还是目录
    //查看路径path
    public static String testListStatus(Path path){
        FileSystem fs = connection.getFs();
        FileStatus[] fileStatuses = null;
        try {
            fileStatuses = fs.listStatus(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        connection.close(fs);
        for (FileStatus fileStatus : fileStatuses) {

            // 如果是文件
            if (fileStatus.isFile()) {
               return "f:"+fileStatus.getPath().getName();
            }else {
               return "d:"+fileStatus.getPath().getName();
            }
        }
        return null;
    }

}
