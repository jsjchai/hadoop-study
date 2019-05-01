package com.jsjchai.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

/**
 * @author jsjchai.
 */
public class WordCountTest {

    /**
     *  配置 vm options:-DHADOOP_USER_NAME=root
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Test
    public void testDoJob() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar","target/mapreduce-1.0-SNAPSHOT.jar");
        conf.set("yarn.application.classpath","/usr/local/hadoop-3.2.0/etc/hadoop:/usr/local/hadoop-3.2.0/share/hadoop/common/lib/*:/usr/local/hadoop-3.2.0/share/hadoop/common/*:/usr/local/hadoop-3.2.0/share/hadoop/hdfs:/usr/local/hadoop-3.2.0/share/hadoop/hdfs/lib/*:/usr/local/hadoop-3.2.0/share/hadoop/hdfs/*:/usr/local/hadoop-3.2.0/share/hadoop/mapreduce/lib/*:/usr/local/hadoop-3.2.0/share/hadoop/mapreduce/*:/usr/local/hadoop-3.2.0/share/hadoop/yarn:/usr/local/hadoop-3.2.0/share/hadoop/yarn/lib/*:/usr/local/hadoop-3.2.0/share/hadoop/yarn/*");
        conf.set("fs.defaultFS","hdfs://master:9000/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","master");
        //如果启用，用户可以跨平台提交应用程序，即从Windows客户机提交应用程序到Linux/Unix服务器，反之亦然
        conf.set("mapreduce.app-submission.cross-platform","true");
        WordCount.doJob(conf,"hdfs://master:9000/wordcount/input","hdfs://master:9000/wordcount/output");
    }

    @Test
    public void testLocalDoJob() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        WordCount.doJob(conf,"G:\\test\\input","G:\\test\\output");
    }
}
