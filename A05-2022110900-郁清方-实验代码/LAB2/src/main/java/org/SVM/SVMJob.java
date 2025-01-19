package org.SVM;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class SVMJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: SVMJob <input path> <output path> <mode> <learning rate> <max iterations>");
            System.exit(-1);
        }

        double learningRate = Double.parseDouble(args[3]);
        int maxIterations = Integer.parseInt(args[4]);

        int iteration = 0;
        boolean converged = false;

        switch (args[2]) {
            case "train":
                while (iteration < maxIterations && !converged) {
                    Configuration conf_train = new Configuration();
                    Job job_train = Job.getInstance(conf_train, "SVM-train" + iteration);

                    job_train.setJarByClass(SVMJob.class);
                    job_train.getConfiguration().setDouble("learningRate", learningRate);
                    job_train.getConfiguration().setInt("iterations", iteration);
                    job_train.getConfiguration().setInt("max_iterations", maxIterations);

                    job_train.setMapperClass(TrainMapper.class);
                    job_train.setReducerClass(TrainReducer.class);
                    job_train.setOutputKeyClass(Text.class);
                    job_train.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job_train, new Path(args[0]));
                    FileOutputFormat.setOutputPath(job_train, new Path(args[1] + "/iteration" + iteration));

                    if (!job_train.waitForCompletion(true)) {
                        break;
                    }
                    // 打印当前损失
                    Path LossPath = new Path(args[1] + "/iteration" + iteration + "/part-r-00000");
                    try {
                        FileSystem fs = FileSystem.get(conf_train);
                        FSDataInputStream inputStream = fs.open(LossPath);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                        String line = reader.readLine();
                        line = reader.readLine(); // Loss被存在第2行，读两次
                        System.out.println("==============================");
                        System.out.println("Iteration: " + iteration);
                        System.out.println("Loss: " + line);
                        System.out.println("==============================");
                        reader.close();
                        inputStream.close();
                        fs.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    iteration++;

                    Path convergenceFlagPath = new Path(args[1] + "/ModelWeights");
                    FileSystem fs = FileSystem.get(conf_train);
                    if (fs.exists(convergenceFlagPath)) {
                        converged = true; // 发现模型标志
                        System.out.println("============================================================");
                        System.out.println("检测到SVM收敛/达到最大迭代次数，停止训练。");
                        System.out.println("============================================================");
                    }
                }
                break;
            case "test":
                // If it's the testing phase, use LogisticRegressionTesterMapper and LogisticRegressionTesterReducer
                Configuration conf_test = new Configuration();
                Job job_test = Job.getInstance(conf_test, "SVM-test");

                job_test.setJarByClass(org.SVM.SVMJob.class);

                job_test.setMapperClass(TesterMapper.class);
                job_test.setReducerClass(TesterReducer.class);
                job_test.setOutputKeyClass(Text.class);
                job_test.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job_test, new Path(args[0]));
                FileOutputFormat.setOutputPath(job_test, new Path(args[1]));

                System.exit(job_test.waitForCompletion(true) ? 0 : 1);
                break;
            case "validate":
                Configuration conf_validation = new Configuration();
                Job job_validation = Job.getInstance(conf_validation, "SVM-validation");

                job_validation.setJarByClass(org.SVM.SVMJob.class);
                job_validation.setMapperClass(ValidationMapper.class);
                job_validation.setReducerClass(ValidationReducer.class);
                job_validation.setOutputKeyClass(Text.class);
                job_validation.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job_validation, new Path(args[0]));
                FileOutputFormat.setOutputPath(job_validation, new Path(args[1]));

                System.exit(job_validation.waitForCompletion(true) ? 0 : 1);
                break;
        }
    }
}


