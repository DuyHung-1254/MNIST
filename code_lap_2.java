import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileInputFormat;

import java.io.IOException;
import org.apache.hadoop.io.it.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.Text;
import org.apache.hadoop.mapreduce.mapreduce.Mapper;


@override
public void map(LongWritable key, Text value, Context context)
    throw IOException, InterruptedException{
    String line = value.toString();
    String[] = rating = line.split("\t");

    if (rating.length >= 4){
        //rating is: [userId, movieId, rating, timestamp]
        int rate = Integer.parseInt(rating[2]);

        //convert to Hadoop types
        IntWritable mapkey = new IntWritable(rate);
        IntWritable mapvalue = new IntWritable(rate);
        
        //Output intermadiate key, value pair
        context.write(mapKey, mapValue);
    }
}


public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
{
    @Override
    public void reduce (IntWritable key, IntWritable<IntWribale> values, Context context) throws IOException , InterruptedException 
    {
        int count = 0 ;
        String title = "";

        // add up all the ratting 
        for (IntWritable value:values)
        {
            count += value.get();

        }

        IntWribale value = new IntWribale(count);

        // output movieid , title and avg rating 
        context.write(key,value);
    }
}