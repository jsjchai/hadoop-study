package com.jsjchai.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author tianhe
 * @date 2019-04-25 16:06
 **/
public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one;
    private final Text word;

    public WordMapper() {
        one = new IntWritable(1);
        word = new Text();
    }


    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}