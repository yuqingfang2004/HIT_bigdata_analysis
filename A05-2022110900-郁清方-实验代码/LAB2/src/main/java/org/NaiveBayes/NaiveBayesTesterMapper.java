package org.NaiveBayes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NaiveBayesTesterMapper extends Mapper<Object, Text, Text, Text> {
    private static final int NUM_FEATURES = 20;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");

        // 将数据写到Reducer
        context.write(new Text("test_data"), new Text(value.toString()));
    }
}


