package org.Missing_Value_Imputation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Missing_Value_Imputation_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\\|");

        String userNationality = fields[9];
        String userCareer = fields[10];
        String userIncome = fields[11];
        String rating = fields[6];

        // 组合，作为key，写入不同的Reducer
        String compositeKey = userNationality + "|" + userCareer;
        context.write(new Text(compositeKey), new Text(value.toString()));
    }
}
