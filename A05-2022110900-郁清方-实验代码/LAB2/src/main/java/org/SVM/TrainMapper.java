package org.SVM;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrainMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 直接写给reducer，写入同一个reducer
        context.write(new Text("train"), value);
    }
}
