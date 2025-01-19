package org.Missing_Value_Imputation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Missing_Value_Imputation_Reducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> records = new ArrayList<>();
        double incomeSum = 0;
        int incomeCount = 0;

        // 首先计算同一国籍和职业的平均收入
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            String userIncome = fields[11];

            // 如果user_income不是缺失值，累计收入
            if (!userIncome.equals("?")) {
                incomeSum += Double.parseDouble(userIncome);
                incomeCount++;
            }
            records.add(value.toString());
        }

        double avgIncome = incomeCount > 0 ? incomeSum / incomeCount : 0;

        // 遍历，开始填充
        for (String record : records) {
            String[] fields = record.split("\\|");
            String userIncome = fields[11];
            String rating = fields[6];

            // 填充user_income，这里使用平均值——Yu
            if (userIncome.equals("?")) {
                fields[11] = String.valueOf(avgIncome);
            }

            // 填充rating，根据线性回归模型结果进行填充
            if (rating.equals("?")) {
                double predictedRating = predictRating(fields);
                fields[6] = String.valueOf(predictedRating);
            }

            // 输出结果
            context.write(NullWritable.get(), new Text(String.join("|", fields)));
        }
    }

    // 线性回归模型的预测函数
    private double predictRating(String[] fields) {
        // 提取用于预测rating的相关字段
        double userIncome = Double.parseDouble(fields[11]);
        double longitude = Double.parseDouble(fields[1]);
        double latitude = Double.parseDouble(fields[2]);
        double altitude = Double.parseDouble(fields[3]);

        double intercept = 0.9380143220692204;
        double[] coefficients = {0.008722841829831903, -0.005970217551254121, 0.015460441087522522, 0.005325407348694155};
        return intercept + coefficients[0] * longitude + coefficients[1] * latitude + coefficients[2] * altitude + coefficients[3] * userIncome;
    }
}
