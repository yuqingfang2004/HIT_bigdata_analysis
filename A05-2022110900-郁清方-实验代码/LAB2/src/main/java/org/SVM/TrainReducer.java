package org.SVM;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

public class TrainReducer extends Reducer<Text, Text, Text, Text> {

    private static double[] weights; // 假设权重数组
    private static double bias;       // 偏置
    private static double learningRate; // 学习率
    private static double previousLoss;
    private int iterations;
    private int max_iterations;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取配置中的学习率和初始化权重
        Configuration conf = context.getConfiguration();
        learningRate = conf.getDouble("learningRate", 0.001); // 设置默认学习率为 0.001
        iterations = conf.getInt("iterations", 0);
        max_iterations = conf.getInt("max_iterations", 10);
        Random rand = new Random();
        weights = new double[20];
        if (iterations == 0) { // 如果是第一次迭代，那么需要初始化SVM的权重和偏置，这里我采用的是均匀分布随机化
            for (int i = 0; i < 20; i++) {
                weights[i] = rand.nextDouble() * 0.1 - 0.05; // 产生在-0.05~0.05之间的均匀分布随机值
            }
            bias = rand.nextDouble() - 0.5;
        } else { // 否则，读取上一次MapReduce产生的相关参数
            String weightsFilePath = "hdfs:///LAB2/SVMTrain/iteration" + (iterations - 1) + "/part-r-00000"; // 从上一次迭代输出的文件位置读取
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(weightsFilePath);
            if (fs.exists(path)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line = reader.readLine();
                if (line != null) {
                    String[] WeightAndBiasStrings = line.split(","); // w1,w2,...,w20,b
                    for (int i = 0; i < WeightAndBiasStrings.length - 1; i++) {
                        weights[i] = Double.parseDouble(WeightAndBiasStrings[i]);
                    }
                    bias = Double.parseDouble(WeightAndBiasStrings[WeightAndBiasStrings.length - 1]); // weight后面紧跟着bias，直接读第20个
                }
                line = reader.readLine();
                if (line != null) {
                    previousLoss = Double.parseDouble(line);
                }
                reader.close();
            } else {
                System.err.println("权重文件不存在！");
            }
        }

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 将梯度初始化
        double[] weightGradients = new double[weights.length];
        double biasGradient = 0.0;
        double loss = 0.0;

        // 遍历每个输入样本并更新梯度
        for (Text value : values) {
            // x1,x2,...,x20,label
            String[] parts = value.toString().split(",");
            double[] features = new double[weights.length];
            double label = Double.parseDouble(parts[features.length]);

            // 将标签0转换为-1，标签1保持为1
            if (label == 0) {
                label = -1;  // 将标签0转换为-1
            }

            // 提取特征
            for (int i = 0; i < features.length; i++) {
                features[i] = Double.parseDouble(parts[i]);
            }

            // 计算模型的线性输出 z = w * x + b
            double z = dotProduct(weights, features) + bias;

            // 计算Hinge损失
            loss += hingeLoss(label, z);

            // 计算梯度，只有在分类错误时才更新权重
            if (label * z < 1) { // 如果是错误分类或者间隔小于1
                for (int i = 0; i < weights.length; i++) {
                    weightGradients[i] += -label * features[i]; // 对应Hinge损失的梯度
                }
                biasGradient += -label; // 偏置梯度
            }
        }

        // 梯度裁剪
        double maxGradient = 1.0;  // 设定一个阈值
        for (int i = 0; i < weightGradients.length; i++) {
            if (weightGradients[i] > maxGradient) {
                weightGradients[i] = maxGradient;
            } else if (weightGradients[i] < -maxGradient) {
                weightGradients[i] = -maxGradient;
            }
        }

        if (biasGradient > maxGradient) {
            biasGradient = maxGradient;
        } else if (biasGradient < -maxGradient) {
            biasGradient = -maxGradient;
        }

        // 更新权重和偏置
        for (int i = 0; i < weights.length; i++) {
            weights[i] -= learningRate * weightGradients[i];
        }
        bias -= learningRate * biasGradient;

        // 计算损失差，判断是否收敛
        double lossDifference = Math.abs(previousLoss - loss);
        if ((iterations + 1) == max_iterations || lossDifference < 0.01) { // 如果损失差小于某个阈值，认为收敛或者达到最大迭代次数
            // 将权重写入model的最终文件
            System.out.println("Saving final model weights to HDFS...");
            Path FinalModelPath = new Path("hdfs:///LAB2/SVMTrain/ModelWeights");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream outStream = fs.create(FinalModelPath);
            StringBuilder FinalModelBuilder = new StringBuilder();
            for (int i = 0; i < weights.length; i++) {
                if (i > 0) {
                    FinalModelBuilder.append(",");
                }
                FinalModelBuilder.append(weights[i]);
            }
            FinalModelBuilder.append(",").append(bias);
            outStream.writeBytes(FinalModelBuilder.toString());
            outStream.close();
        }

        System.out.println("========================================");
        System.out.println("Iteration: " + iterations);
        System.out.println("Loss: " + loss);
        System.out.println("Weight gradients: " + Arrays.toString(weightGradients));
        System.out.println("Bias gradient: " + biasGradient);
        System.out.println("Updated weights: " + Arrays.toString(weights));
        System.out.println("========================================");

        // 将权重和偏置拼接成一个字符串，权重之间用逗号分隔，最后加上偏置
        StringBuilder modelBuilder = new StringBuilder();
        for (int i = 0; i < weights.length; i++) {
            if (i > 0) {
                modelBuilder.append(",");
            }
            modelBuilder.append(weights[i]);
        }
        modelBuilder.append(",").append(bias);  // 将偏置直接追加到权重后面
        context.write(new Text(""), new Text(modelBuilder.toString()));
        context.write(new Text(""), new Text(Double.toString(loss))); // 第二行写loss
    }


    // 计算 Hinge 损失
    private double hingeLoss(double label, double z) {
        return Math.max(0, 1 - label * z);
    }

    // 计算权重和特征的点积
    private double dotProduct(double[] weights, double[] features) {
        double result = 0.0;
        for (int i = 0; i < weights.length; i++) {
            result += weights[i] * features[i];
        }
        return result;
    }
}
