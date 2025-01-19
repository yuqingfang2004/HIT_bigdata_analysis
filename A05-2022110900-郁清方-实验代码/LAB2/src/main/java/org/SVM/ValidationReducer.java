package org.SVM;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValidationReducer extends Reducer<Text, Text, Text, Text> {

    private int totalSamples = 0;
    private int correctPredictions = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double label = Double.parseDouble(key.toString());
        int labelInt = (int) label;
        for (Text value : values) {
            int predictedLabel = Integer.parseInt(value.toString());
            totalSamples++;
            if (label == predictedLabel) {
                correctPredictions++;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double accuracy = (double) correctPredictions / totalSamples * 100;
        context.write(new Text("Accuracy"), new Text(String.format("%.2f%%", accuracy)));
    }
}
