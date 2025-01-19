package org.Filter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Filter_Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 遍历所有来自 Mapper 的值并输出——Yu
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
