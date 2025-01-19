package org.Filter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Filter_Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static final double LONGITUDE_MIN = 8.1461259;
    private static final double LONGITUDE_MAX = 11.1993265;
    private static final double LATITUDE_MIN = 56.5824856;
    private static final double LATITUDE_MAX = 57.750511;   //先设置好有效范围——Yu

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length >= 12) {
            double longitude = Double.parseDouble(fields[1]);
            double latitude = Double.parseDouble(fields[2]);//对应2和3列，从数组来看就是对应下标1和2——Yu

            // 检查值是否在有效范围内，如果符合要求，就输出——Yu
            if ((longitude >= LONGITUDE_MIN && longitude <= LONGITUDE_MAX) &&
                    (latitude >= LATITUDE_MIN && latitude <= LATITUDE_MAX)) {
                context.write(NullWritable.get(), value); // 输出原始数据
            }
        }
    }
}

