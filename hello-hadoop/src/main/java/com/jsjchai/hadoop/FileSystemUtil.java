package com.jsjchai.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author jsjchai.
 */
public class FileSystemUtil {

    /**
     * 获取hdfs文件系统对象
     * @return FileSystem
     * @throws  IOException
     */
    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs","hdfs://master:9000");
        return FileSystem.get(conf);
    }

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystemUtil.getFileSystem();
        System.out.println(fileSystem.exists(new Path("/hadoop")));
    }
}
