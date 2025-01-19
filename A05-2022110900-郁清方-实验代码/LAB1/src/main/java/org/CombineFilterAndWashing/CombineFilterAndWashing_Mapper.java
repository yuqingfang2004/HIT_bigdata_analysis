package org.CombineFilterAndWashing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CombineFilterAndWashing_Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static final double LONGITUDE_MIN = 8.1461259;
    private static final double LONGITUDE_MAX = 11.1993265;
    private static final double LATITUDE_MIN = 56.5824856;
    private static final double LATITUDE_MAX = 57.750511;   //先设置好有效范围——Yu

    private static final SimpleDateFormat[] dateFormats = new SimpleDateFormat[]{
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy/MM/dd"),
            new SimpleDateFormat("MMMM dd,yyyy", Locale.ENGLISH)
    };

    private static final SimpleDateFormat targetDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\\|");

        double longitude = Double.parseDouble(fields[1]);
        double latitude = Double.parseDouble(fields[2]);//对应2和3列，从数组来看就是对应下标1和2——Yu

        try {
            fields[4] = convertDate(fields[4]);
            fields[8] = convertDate(fields[8]);

            System.out.println("Original temp: " + fields[5] + ", Converted temp: " + convertTemperatureToCelsius(fields[5]));

            fields[5] = convertTemperatureToCelsius(fields[5]);
            if ((longitude >= LONGITUDE_MIN && longitude <= LONGITUDE_MAX) &&
                    (latitude >= LATITUDE_MIN && latitude <= LATITUDE_MAX)) {
                context.write(NullWritable.get(), new Text(String.join("|", fields)));// 这里写之前再检查奇异值的问题——Yu
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private String convertDate(String date) throws ParseException {
        for (SimpleDateFormat format : dateFormats) {
            try {
                Date parsedDate = format.parse(date);
                return targetDateFormat.format(parsedDate);
            } catch (ParseException ignored) {
            }
        }
        throw new ParseException("Unrecognized date format: " + date, 0);
    }

    private String convertTemperatureToCelsius(String temp) {
        if (temp.toUpperCase().contains("℉")) {
            double fahrenheit = Double.parseDouble(temp.replaceAll("[^\\d.]", ""));
            double celsius = (fahrenheit - 32) * 5 / 9.0;
            return String.format("%.2f℃", celsius);
        }
        return temp;
    }
}


