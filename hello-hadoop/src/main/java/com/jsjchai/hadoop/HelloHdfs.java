package com.jsjchai.hadoop;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author jsjchai.
 */
public class HelloHdfs {


    public static void main(String[] args) {

        InputStream in = null;
        try {
            /*设置URL支持hdfs协议*/
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
            URL url = new URL("hdfs://master:9000/test/hello.txt");
            in = url.openStream();
            IOUtils.copy(in, System.out);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }
}
