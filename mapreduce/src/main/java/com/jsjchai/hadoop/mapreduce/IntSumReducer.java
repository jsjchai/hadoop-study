package com.jsjchai.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author jsjchai
 * @date 2019-05-01 11:07
 **/
public class IntSumReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result;

    public IntSumReducer() {
        result = new IntWritable();
    }

    @Override
    public void reduce(Text	key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        int sum = 0;
        for(IntWritable val:values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
