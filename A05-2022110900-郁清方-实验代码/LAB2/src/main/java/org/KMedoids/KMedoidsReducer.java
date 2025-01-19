package org.KMedoids;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

public class KMedoidsReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, double[]> newCentroids = new HashMap<>();
    private int iteration;
    private int PAMIndex; // 抽样总数

    @Override
    protected void setup(Context context) {
        // 获取当前迭代次数
        this.iteration = context.getConfiguration().getInt("iteration", 0);
        // 获取抽样总数限制，默认1000
        this.PAMIndex = context.getConfiguration().getInt("PAMIndex", 1000);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<double[]> points = new ArrayList<>();

        // 收集所有属于该簇的点
        for (Text value : values) {
            double[] point = parseVector(value.toString());
            points.add(point);
        }

        // 更新聚类中心，使用基于抽样的 PAM 方法
        double[] newCentroid = findMedoidWithSampling(points);

        // 存储新的聚类中心
        newCentroids.put(key.toString(), newCentroid);
    }

    private double[] findMedoidWithSampling(List<double[]> points) {
        double minTotalDistance = Double.MAX_VALUE;
        double[] medoid = null;

        // 根据抽样比例选择候选 Medoid
        List<double[]> sampledPoints = samplePoints(points, PAMIndex);

        // 对于每个候选点，计算它到其他所有点的总距离，选择距离最小的点作为中心
        for (double[] candidate : sampledPoints) {
            double totalDistance = 0.0;
            for (double[] otherPoint : points) {
                totalDistance += calculateDistance(candidate, otherPoint);
            }
            if (totalDistance < minTotalDistance) {
                minTotalDistance = totalDistance;
                medoid = candidate;
            }
        }
        return medoid;
    }

    private List<double[]> samplePoints(List<double[]> points, int maxSamples) {
        List<double[]> sampled = new ArrayList<>();
        Random random = new Random();
        int totalPoints = points.size();
        for (int i = 0; i < Math.min(maxSamples, totalPoints); i++) {
            sampled.add(points.get(random.nextInt(totalPoints)));
        }
        return sampled.isEmpty() ? points : sampled;
    }


    private double calculateDistance(double[] point, double[] centroid) {
        double sum = 0.0;
        for (int i = 0; i < point.length; i++) {
            sum += Math.pow(point[i] - centroid[i], 2);
        }
        return Math.sqrt(sum);
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
        String centroidsPath = "hdfs:///LAB2/KMedoidsCentroids" + "/centroids" + iteration;
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
