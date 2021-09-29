import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank {

	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			//Splitting the node/page from pages on the right separated by tab
			String[] nodes = value.toString().split("\t");
			//NodeId is the page we are working on
			String nodeId = nodes[0];
			//Initial Rank
			double initRank = 1.0;
			//We are writing the initial node that we are working on and its rank
			
			//context.write(new Text("Yoooooo"),new DoubleWritable(initRank));
			
			//Splitting all the pages on the right of the nodeId node
			String[] outLinks = nodes[1].split(",");	
			//This is the part of the equation that says PR(T1)/C(T1) --> C stands for cardinality
			double ratio = initRank / outLinks.length;			
			//Prints the ratio for each page, but does not do any computation with it
				context.write(new Text(nodeId +' '+ Integer.toString(outLinks.length)), new DoubleWritable(ratio));
			
		}
	}

	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		//Using for sorting function
		private Map<Text, DoubleWritable> countMap = new HashMap<Text, DoubleWritable>();
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			double pagerank = 0.0;
			for (DoubleWritable value : values) {
				pagerank += Double.parseDouble(value.toString());
			}
			//updating the formula to be (1-d) + d(PR(a)/C(a)...) , where d = 0.85
			pagerank = 0.15 + 0.85 * pagerank;
			//context.write(key, new DoubleWritable(pagerank));
			countMap.put(new Text(key), new DoubleWritable(pagerank));
		}
		
		//Sorting
		public void cleanup(Context context) throws IOException, InterruptedException {
		  List<Entry<Text, DoubleWritable>> countList = new ArrayList<Entry<Text, DoubleWritable>>(countMap.entrySet());
		  Collections.sort( countList, new Comparator<Entry<Text, DoubleWritable>>(){
		        public int compare( Entry<Text, DoubleWritable> o1, Entry<Text, DoubleWritable> o2 ) {
		            return (o2.getValue()).compareTo( o1.getValue() );
		        }
		    } );
			int i=0;
			for(Entry<Text, DoubleWritable> entry: countList) {
			    	
		    	context.write(entry.getKey(), entry.getValue());
			i++;
			if(i==500){break;}
		    }	
		}
	}

	
	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PageRank.class);
		job.setJobName("Page Rank");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
