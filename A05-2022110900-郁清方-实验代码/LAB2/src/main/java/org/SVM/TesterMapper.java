package org.SVM;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TesterMapper extends Mapper<Object, Text, Text, Text> {

    private double[] weights; // 权重数组
    private double bias;       // 偏置

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String modelPath = "hdfs:///LAB2/SVMTrain/ModelWeights";

        // 从HDFS中读取模型参数
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(modelPath);
        if (fs.exists(path)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = reader.readLine();
            if (line != null) {
                String[] params = line.split(",");
                weights = new double[params.length - 1];
                for (int i = 0; i < weights.length; i++) {
                    weights[i] = Double.parseDouble(params[i]);
                }
                bias = Double.parseDouble(params[params.length - 1]);
            }
            reader.close();
        } else {
            throw new IOException("模型参数文件不存在: " + modelPath);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 读取测试数据，每行格式：x1,x2,...,x20
        String[] parts = value.toString().split(",");
        double[] features = new double[weights.length];

        for (int i = 0; i < weights.length; i++) {
            features[i] = Double.parseDouble(parts[i]);
        }

        // 计算模型预测值
        double z = dotProduct(weights, features) + bias;
        double predictedProb = sigmoid(z);
        int predictedLabel = predictedProb >= 0.5 ? 1 : 0;

        // value为预测标签
        context.write(new Text(""), new Text(Integer.toString(predictedLabel)));
    }

    // 计算Sigmoid函数
    private double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }

    // 计算点积
    private double dotProduct(double[] weights, double[] features) {
        double result = 0.0;
        for (int i = 0; i < weights.length; i++) {
            result += weights[i] * features[i];
        }
        return result;
    }
}
