package org.NaiveBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveBayesJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: NaiveBayesJob <input path> <output path> <mode>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NaiveBayes");

        job.setJarByClass(NaiveBayesJob.class);

        switch (args[2]) {
            case "train":
                job.setMapperClass(NaiveBayesTrainerMapper.class);
                job.setReducerClass(NaiveBayesTrainerReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case "test":
                job.setMapperClass(NaiveBayesTesterMapper.class);
                job.setReducerClass(NaiveBayesTesterReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case "validate":
                job.setMapperClass(NaiveBayesValidationMapper.class);
                job.setReducerClass(NaiveBayesValidationReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

