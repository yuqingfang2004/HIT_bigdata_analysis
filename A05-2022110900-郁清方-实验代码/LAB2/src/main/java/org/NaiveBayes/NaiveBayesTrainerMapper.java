package org.NaiveBayes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NaiveBayesTrainerMapper extends Mapper<Object, Text, Text, Text> {
    private static final int NUM_FEATURES = 20;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");

        int ClassLabel = Integer.parseInt(tokens[NUM_FEATURES]);

        // 按照不同类别传递给Reducer
        context.write(new Text("class_" + ClassLabel), new Text(line));
    }
}


