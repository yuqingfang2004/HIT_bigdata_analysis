package org.Missing_Value_Imputation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class Missing_Value_Imputation_Job {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法: org.Missing_Value_Imputation.Missing_Value_Imputation_Job <输入路径> <输出路径>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Income Imputation");

        job.setJarByClass(Missing_Value_Imputation_Job.class);
        job.setMapperClass(Missing_Value_Imputation_Mapper.class); // 使用相同的Mapper处理逻辑
        job.setReducerClass(Missing_Value_Imputation_Reducer.class);  // 特定的Reducer来填充收入

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

