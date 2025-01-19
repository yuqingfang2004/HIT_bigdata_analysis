package org.KMedoids;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.*;

public class KMedoidsJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("输入: KMedoidsJob <输入> <输出> <分类> <最大迭代次数> <收敛值> <PAM抽样个数>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int K = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);
        double convergeIndex = Double.parseDouble(args[4]);
        int PAMIndex = Integer.parseInt(args[5]);

        Configuration conf = new Configuration();

        // 生成初始中心点，我是选择从点当中随机抽取K个——Yu
        String centroidsPath = generateInitialCentroids(conf, inputPath, K);
        conf.set("centroids.path", centroidsPath);

        boolean converged = false;
        int iteration = 0;

        while (iteration < maxIterations && !converged) {
            Job job = Job.getInstance(conf, "KMedoids Clustering - Iteration " + iteration);

            job.setJarByClass(org.KMedoids.KMedoidsJob.class);
            job.setMapperClass(KMedoidsMapper.class);
            job.setReducerClass(KMedoidsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath + "/centroids" + iteration));

            job.addCacheFile(new URI(centroidsPath));
            job.getConfiguration().setInt("iteration", iteration);
            job.getConfiguration().setInt("PAMIndex", PAMIndex);
            job.getConfiguration().set("outputPath", String.valueOf(new Path(outputPath + "/centroids" + iteration)));

            if (!job.waitForCompletion(true)) {
                System.err.println("Job failed at iteration " + iteration);
                System.exit(1);
            }

            String newCentroidsPath = "hdfs:///LAB2/KMedoidsCentroids" + "/centroids" + iteration;

            // 检查是否收敛
            if (iteration != 0) { // 第一次迭代我认为只有很小概率会直接收敛，所以就不检查了——Yu
                converged = checkConvergence(conf, centroidsPath, newCentroidsPath, convergeIndex);
            }

            centroidsPath = newCentroidsPath;
            conf.set("centroids.path", centroidsPath);
            iteration++;
        }
        System.out.println("========================================");
        System.out.println("KMedoids 在 " + iteration + " 次迭代时收敛");
        System.out.println("========================================");

        // 收敛后或达到最大收敛次数时进行数据聚类，输出结果
        Job finalJob = Job.getInstance(conf, "Final Clustering");

        finalJob.setJarByClass(org.KMedoids.KMedoidsJob.class);
        finalJob.setMapperClass(FinalKMedoidsMapper.class);
        finalJob.setReducerClass(FinalKMedoidsReducer.class);

        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(Text.class);

        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(finalJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(finalJob, new Path(outputPath + "/final_results"));

        finalJob.addCacheFile(new URI(centroidsPath));

        if (!finalJob.waitForCompletion(true)) {
            System.err.println("Final Job failed");
            System.exit(1);
        }

    }

    private static String generateInitialCentroids(Configuration conf, String inputPath, int K) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path centroidsPath = new Path("hdfs:///LAB2/KMedoidsCentroids/initial_centroids.txt");

        Random rand = new Random();
        List<String> selectedCentroids = new ArrayList<>(K);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                count++;
                if (count <= K) {
                    selectedCentroids.add(line);
                } else {
                    int replace = rand.nextInt(count);
                    if (replace < K) {
                        selectedCentroids.set(replace, line);
                    }
                }
            }
        }
        try (OutputStreamWriter writer = new OutputStreamWriter(fs.create(centroidsPath, true))) {
            int count = 0;
            for (String centroid : selectedCentroids) {
                writer.write(count + "\t" + centroid + "\n");
                count++;
            }
        }
        return centroidsPath.toString();
    }

    private static boolean checkConvergence(Configuration conf, String oldCentroidsPath, String newCentroidsPath, double convergeIndex) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Map<String, double[]> oldCentroids = readCentroidsFromHDFS(fs, new Path(oldCentroidsPath));
        Map<String, double[]> newCentroids = readCentroidsFromHDFS(fs, new Path(newCentroidsPath));

        for (String centroidId : newCentroids.keySet()) {
            double[] oldPoint = oldCentroids.get(centroidId);
            double[] newPoint = newCentroids.get(centroidId);

            if (oldPoint == null || newPoint == null) {
                return false;
            }

            double distance = calculateDistance(oldPoint, newPoint);
            if (distance > convergeIndex) {
                return false;
            }
        }

        return true;
    }

    private static Map<String, double[]> readCentroidsFromHDFS(FileSystem fs, Path path) throws Exception {
        Map<String, double[]> centroids = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String centroidId = parts[0];
                double[] vector = parseVector(parts[1]);
                centroids.put(centroidId, vector);
            }
        }

        return centroids;
    }

    private static double[] parseVector(String data) {
        String[] parts = data.split(",");
        double[] vector = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vector[i] = Double.parseDouble(parts[i]);
        }
        return vector;
    }

    private static double calculateDistance(double[] point1, double[] point2) {
        double sum = 0.0;
        for (int i = 0; i < point1.length; i++) {
            sum += Math.pow(point1[i] - point2[i], 2);
        }
        return Math.sqrt(sum);
    }
}

