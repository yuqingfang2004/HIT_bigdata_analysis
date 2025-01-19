package org.NaiveBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class NaiveBayesValidationReducer extends Reducer<Text, Text, Text, Text> {
    private static final int NUM_FEATURES = 20;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 加载训练阶段的模型数据
        Map<Integer, ModelData> modelDataMap = loadModelData();
        int correctCount = 0;
        int totalCount = 0;

        for (Text value : values) {
            String line = value.toString();
            String[] tokens = line.split(",");
            double[] features = new double[NUM_FEATURES];
            int trueLabel = Integer.parseInt(tokens[NUM_FEATURES]);

            for (int i = 0; i < NUM_FEATURES; i++) {
                features[i] = Double.parseDouble(tokens[i]);
            }

            // 计算每个类别的后验概率
            double maxPosterior = Double.NEGATIVE_INFINITY;
            int predictedClass = -1;

            for (int classLabel : modelDataMap.keySet()) {
                ModelData modelData = modelDataMap.get(classLabel);
                double prior = modelData.getPrior();
                double likelihood = calculateLikelihood(features, modelData.getMeans(), modelData.getVariances());

                // 计算后验概率
                double posterior = Math.log(likelihood) + Math.log(prior);
                // 选择概率大的作为预测结果
                if (posterior > maxPosterior) {
                    maxPosterior = posterior;
                    predictedClass = classLabel;
                }
            }

            // 统计正确分类的数量
            if (predictedClass == trueLabel) {
                correctCount++;
            }
            totalCount++;
        }

        // 输出验证集上的分类准确率
        double accuracy = (double) correctCount / totalCount * 100;
        context.write(new Text("Accuracy"), new Text(String.format("%.2f%%", accuracy)));
    }

    private double calculateLikelihood(double[] features, double[] means, double[] variances) {
        double likelihood = 1.0;
        // 使用高斯朴素贝叶斯计算
        for (int i = 0; i < features.length; i++) {
            double exponent = Math.exp(-(Math.pow(features[i] - means[i], 2) / (2 * variances[i])));
            likelihood *= exponent / Math.sqrt(2 * Math.PI * variances[i]);
        }
        return likelihood;
    }

    private Map<Integer, ModelData> loadModelData() {
        Map<Integer, ModelData> modelDataMap = new HashMap<>();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            // 模型文件路径
            Path modelPath = new Path("hdfs:///LAB2/NaiveBayesModel/part-r-00000");
            FSDataInputStream fsDataInputStream = fs.open(modelPath);

            // 读取文件
            BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line;

            while ((line = reader.readLine()) != null) {
                // 使用空格分隔字段
                String[] parts = line.split("\\s+");

                // 获取标签
                String classLabelStr = parts[0];
                int classLabel = Integer.parseInt(classLabelStr.split("_")[1]); // 获取数字标签，比如 class_0 -> 0

                // 获取先验概率
                String[] stats = parts[1].split(",");
                double prior = Double.parseDouble(stats[0]);

                // 获取均值和方差
                double[] means = new double[NUM_FEATURES];
                double[] variances = new double[NUM_FEATURES];

                // 解析均值和方差（逗号分隔的部分）
                for (int i = 0; i < NUM_FEATURES; i++) {
                    means[i] = Double.parseDouble(stats[2 * i + 1]); // 均值
                    variances[i] = Double.parseDouble(stats[2 * i + 2]); // 方差
                }

                // 将数据封装到 ModelData 中
                ModelData modelData = new ModelData(prior, means, variances);

                // 存入 Map
                modelDataMap.put(classLabel, modelData);
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return modelDataMap;
    }
}


