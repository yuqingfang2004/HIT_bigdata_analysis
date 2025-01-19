package org.CombineFilterAndWashing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombineFilterAndWashing_Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private double minRating = Double.MAX_VALUE;
    private double maxRating = Double.MIN_VALUE;

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String[]> records = new ArrayList<>();

        // 先找最小和最大的rating值——Yu
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            try {
                double rating = Double.parseDouble(fields[6]);
                minRating = Math.min(minRating, rating);
                maxRating = Math.max(maxRating, rating);
            } catch (NumberFormatException e) {
                // 处理无效的rating值
                System.err.println("Invalid rating value: " + fields[6]);
                records.add(fields);//把无效的rating值不做处理，直接加入records里面——Yu
                continue;
            }
            records.add(fields);
        }

        // 归一化处理
        for (String[] fields : records) {
            try{
                double rating = Double.parseDouble(fields[6]);
                fields[6] = normalizeRating(rating);
            } catch (NumberFormatException e) {
                context.write(NullWritable.get(), new Text(String.join("|", fields)));//遇到缺失的rating值，直接写——Yu
                continue;
            }
            context.write(NullWritable.get(), new Text(String.join("|", fields)));
        }
    }

    // 归一化rating
    private String normalizeRating(double rating) {
        return String.format("%.4f", (rating - minRating) / (maxRating - minRating));
    }
}

