package org.CombineFilterAndWashing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombineFilterAndWashing_Job {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法: org.CombineFilterAndWashing.CombineFilterAndWashing_Job <输入路径> <输出路径>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CombineFilterAndWashing");

        job.setJarByClass(org.CombineFilterAndWashing.CombineFilterAndWashing_Job.class);
        job.setMapperClass(CombineFilterAndWashing_Mapper.class);
        job.setReducerClass(CombineFilterAndWashing_Reducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
