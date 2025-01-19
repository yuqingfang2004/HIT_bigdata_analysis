package org.Stratified_Sampling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stratified_Sampling_Job {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法: org.Stratified_Sampling.Stratified_Sampling_Job <输入路径> <输出路径>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stratified Sampling Job");

        job.setJarByClass(Stratified_Sampling_Job.class);
        job.setMapperClass(Stratified_Sampling_Mapper.class);
        job.setReducerClass(Stratified_Sampling_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

