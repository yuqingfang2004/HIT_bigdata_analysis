package org.NaiveBayes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class NaiveBayesTrainerReducer extends Reducer<Text, Text, Text, Text> {
    private static final int NUM_FEATURES = 20;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 统计数据的数量
        int totalCount = 900000;

        // 收集每个类别的所有数据
        List<double[]> classDataList = new ArrayList<>();
        for (Text value : values) {
            String line = value.toString();
            String[] tokens = line.split(",");

            // 提取特征
            double[] features = new double[NUM_FEATURES];
            for (int i = 0; i < NUM_FEATURES; i++) {
                features[i] = Double.parseDouble(tokens[i]);
            }

            // 将数据添加到类别数据列表
            classDataList.add(features);
        }

        // 计算先验概率
        double prior = (double) classDataList.size() / totalCount;

        double[] means = new double[NUM_FEATURES];
        double[] variances = new double[NUM_FEATURES];

        // 计算均值
        for (double[] features : classDataList) {
            for (int i = 0; i < NUM_FEATURES; i++) {
                means[i] += features[i];
            }
        }
        for (int i = 0; i < NUM_FEATURES; i++) {
            means[i] /= classDataList.size();
        }

        // 计算方差
        for (double[] features : classDataList) {
            for (int i = 0; i < NUM_FEATURES; i++) {
                variances[i] += Math.pow(features[i] - means[i], 2);
            }
        }
        for (int i = 0; i < NUM_FEATURES; i++) {
            variances[i] /= classDataList.size();
        }

        // 构建输出字符串
        StringBuilder output = new StringBuilder();
        output.append(prior).append(",");
        for (int i = 0; i < NUM_FEATURES; i++) {
            output.append(means[i]).append(",").append(variances[i]).append(",");
        }

        context.write(new Text("class_" + key.toString().split("_")[1]), new Text(output.toString()));
    }
}

