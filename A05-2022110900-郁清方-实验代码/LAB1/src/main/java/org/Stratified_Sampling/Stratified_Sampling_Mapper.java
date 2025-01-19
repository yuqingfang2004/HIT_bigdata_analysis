package org.Stratified_Sampling;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Stratified_Sampling_Mapper extends Mapper<Object, Text, Text, Text> {
    private final Text userCareer = new Text();
    private final Text dataLine = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\\|");
        if (fields.length >= 12) {
            userCareer.set(fields[10]); // user_career
            dataLine.set(line);
            context.write(userCareer, dataLine); // 输出userCareer和整行数据
        }
    }
}


