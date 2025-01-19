package org.KMedoids;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMedoidsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, double[]> centroids = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        // 加载聚类中心
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            Path centroidsPath = new Path(cacheFiles[0].toString());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)))) {
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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        try {
            double[] point = parseVector(line);
            String closestCentroid = null;
            double minDistance = Double.MAX_VALUE;

            // 计算数据点到各聚类中心的距离
            for (Map.Entry<String, double[]> entry : centroids.entrySet()) {
                double distance = calculateDistance(point, entry.getValue());
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = entry.getKey();
                }
            }

            // 输出最近的聚类中心及数据点
            if (closestCentroid != null) {
                context.write(new Text(closestCentroid), value); // <聚类中心ID, 数据点>
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid input point: " + line);
        }
    }

    private static double[] parseVector(String data) {
        String[] parts = data.split(",");
        double[] vector = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                vector[i] = Double.parseDouble(parts[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid vector format: " + data, e);
            }
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

