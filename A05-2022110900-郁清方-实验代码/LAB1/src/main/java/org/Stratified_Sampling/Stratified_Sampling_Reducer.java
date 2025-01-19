package org.Stratified_Sampling;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Stratified_Sampling_Reducer extends Reducer<Text, Text, NullWritable, Text> {
    private static final double SAMPLE_RATIO = 0.1; // 抽取比例——Yu
    private Random random = new Random();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> records = new ArrayList<>();
        for (Text value : values) {
            records.add(value.toString());
        }
        if (records.isEmpty()) return; // 防止空记录

        // 计算目标样本大小
        int targetSampleSize = (int) Math.ceil(records.size() * SAMPLE_RATIO);
        List<String> sampledRecords = new ArrayList<>();

        // 初步抽样
        for (String record : records) {
            if (random.nextDouble() < (double) targetSampleSize / records.size()) {
                sampledRecords.add(record);
            }
        }

        // 补充抽样
        while (sampledRecords.size() < targetSampleSize && sampledRecords.size() < records.size()) {
            int index = random.nextInt(records.size());
            String record = records.get(index);
            if (!sampledRecords.contains(record)) {
                sampledRecords.add(record);
            }
        }

        // 输出样本
        for (String record : sampledRecords) {
            context.write(NullWritable.get(), new Text(record));
        }
    }
}



