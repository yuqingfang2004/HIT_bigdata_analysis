package org.KMeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {
    private static final int DIMENSIONS = 20;  // 数据维度为20
    private Map<String, double[]> newCentroids = new HashMap<>();
    private int iteration;

    @Override
    protected void setup(Context context) {
        // 获取当前迭代次数
        this.iteration = context.getConfiguration().getInt("iteration", 0);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<double[]> points = new ArrayList<>();

        // 收集所有属于该簇的点
        for (Text value : values) {
            double[] point = parseVector(value.toString());
            points.add(point);
        }

        // 更新聚类中心
        double[] newCentroid = calculateNewCentroid(points);

        // 存储新的聚类中心
        newCentroids.put(key.toString(), newCentroid);
    }

    private double[] calculateNewCentroid(List<double[]> points) {
        double[] centroid = new double[DIMENSIONS];
        int count = points.size();

        // 计算该簇的新的聚类中心
        for (double[] point : points) {
            for (int i = 0; i < point.length; i++) {
                centroid[i] += point[i];
            }
        }

        // 平均化坐标
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] /= count;
        }

        return centroid;
    }

    private double[] parseVector(String data) {
        String[] parts = data.split(",");
        double[] vector = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vector[i] = Double.parseDouble(parts[i]);
        }
        return vector;
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        // 在 cleanup 中根据当前的迭代次数保存新的聚类中心到 HDFS
        String centroidsPath = "hdfs:///LAB2/KMeansCentroids" + "/centroids" + iteration;
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputPath = new Path(centroidsPath);

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)))) {
            for (Map.Entry<String, double[]> entry : newCentroids.entrySet()) {
                String centroidId = entry.getKey();
                double[] centroid = entry.getValue();
                writer.write(centroidId + "\t" + vectorToString(centroid) + "\n");
            }
        }
    }

    private String vectorToString(double[] vector) {
        StringJoiner joiner = new StringJoiner(",");
        for (double v : vector) {
            joiner.add(String.valueOf(v));
        }
        return joiner.toString();
    }
}
