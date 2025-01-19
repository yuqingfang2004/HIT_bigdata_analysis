package org.Washing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Washing_Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {

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

        try {
            fields[4] = convertDate(fields[4]);
            fields[8] = convertDate(fields[8]);

            System.out.println("Original temp: " + fields[5] + ", Converted temp: " + convertTemperatureToCelsius(fields[5]));

            fields[5] = convertTemperatureToCelsius(fields[5]);

            context.write(NullWritable.get(), new Text(String.join("|", fields)));
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


