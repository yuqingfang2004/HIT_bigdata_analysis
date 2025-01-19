package org.SVM;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TesterReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key 是行号，value 是预测标签
        for (Text value : values) {
            context.write(new Text(""), value); // 每行输出一个标签
        }
    }
}
