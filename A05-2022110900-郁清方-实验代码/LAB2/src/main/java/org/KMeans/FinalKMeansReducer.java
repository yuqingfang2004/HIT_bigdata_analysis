package org.KMeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FinalKMeansReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, double[]> centroids = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        // 加载最新的聚类中心
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = new Path(cacheFiles[0].toString());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    String centroidId = parts[0];
                    double[] vector = parseVector(parts[1]);
                    centroids.put(centroidId, vector);
                }
            }
        } else {
            throw new IOException("Centroids file not found in distributed cache.");
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 计算每个点与聚类中心的距离
        for (Text value : values) {
            String line = value.toString().trim();
            if (line.isEmpty()) continue;

            double[] point = parseVector(line);
            String closestCentroid = null;
            double minDistance = Double.MAX_VALUE;

            for (Map.Entry<String, double[]> entry : centroids.entrySet()) {
                double distance = calculateDistance(point, entry.getValue());
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = entry.getKey();
                }
            }

            if (closestCentroid != null) {
                context.write(new Text(""), new Text(closestCentroid));  // 按照<空, 聚类标签>输出
            } else {
                System.err.println("No valid centroid found for point: " + line);
            }
        }
    }

    private double[] parseVector(String data) {
        String[] parts = data.split(",");
        double[] vector = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vector[i] = Double.parseDouble(parts[i].trim());
        }
        return vector;
    }

    private double calculateDistance(double[] point, double[] centroid) {
        double sum = 0.0;
        for (int i = 0; i < point.length; i++) {
            sum += Math.pow(point[i] - centroid[i], 2);
        }
        return Math.sqrt(sum);
    }

}
