package com.utils.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * @author 王继昌
 * @create 2020-09-04 18:38
 */
public class connection {

    private static FileSystem fs;

    public static FileSystem getFs(){
        if (fs != null) {
            return fs;
        }
        try {
            Properties properties = new Properties();
            FileReader fileReader = new FileReader("G:\\idea\\works\\profile02\\lib\\hdfs.properties");
            properties.load(fileReader);
            String url = properties.getProperty("url");
            String user = properties.getProperty("user");
            Configuration configuration = new Configuration();
            fs = FileSystem.get(new URI(url), configuration, "user");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fs;
    }

    public static void close(FileSystem fs){
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
    }



}
