import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Following_cp {
      
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String token1 = tokenizer.nextToken();
	String token2 = tokenizer.nextToken();
        context.write(new IntWritable(Integer.parseInt(token1)), new Text(token2));
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
    private MapWritable arry = new MapWritable();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        String following = "";
        for (Text val : values) {
	    if (following.equals("")){
	        following = following + val;
	    }
 	    else {
	        following = following + "," + val;
	    }
	    
        }
        context.write(key, new Text(following));
    }
 }
        
 public static class map extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String token1 = tokenizer.nextToken();
	String token2 = tokenizer.nextToken();
        context.write(new IntWritable(Integer.parseInt(token2)), new Text(token1));
    }
 } 
        
 public static class reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
    private MapWritable arry = new MapWritable();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        String following = "";
        for (Text val : values) {
	    if (following.equals("")){
	        following = following + val;
	    }
 	    else {
	        following = following + "," + val;
	    }
	    
        }
        context.write(key, new Text(following));
    }
 }
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "Following");
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(Following_cp.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
        
    Job job2 = new Job(conf2, "Follower");
    
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
 	       
    job2.setMapperClass(map.class);
    job2.setReducerClass(reduce.class);
    job2.setJarByClass(Following_cp.class);
        
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
    job2.waitForCompletion(true);
 }
        
}
