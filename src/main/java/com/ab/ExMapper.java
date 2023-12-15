package com.ab;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class ExMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable>{
    private Text word = new Text();
    private DoubleWritable rating = new DoubleWritable();
    public void map(LongWritable key, Text value,OutputCollector<Text,DoubleWritable> output,
                    Reporter reporter) throws IOException{         
                        
            String line = value.toString();
            StringTokenizer  tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                line = tokenizer.nextToken();
                String[] lineValue = line.toString().split(",");
                word.set(new Text(lineValue[1]));
                rating.set(Double.parseDouble(lineValue[2]));
                // key -> movie id, value -> rating
                output.collect(word, rating);
            }
    }
}