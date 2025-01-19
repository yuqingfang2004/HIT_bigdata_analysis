package org.KMedoids;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalKMedoidsMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text(""), value);
    }
}
