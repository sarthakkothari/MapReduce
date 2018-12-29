package tc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import tc.IntPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterEdges extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(TwitterEdges.class);
	private final static IntWritable MAX = new IntWritable(48000);
	
	
	public static enum UpdateCounter {
		  UPDATED;
		}


	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntPair> {
		private int from;
		private int to;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while (itr.hasMoreTokens()) {
				from = Integer.parseInt(itr.nextToken()); // from node
				to = Integer.parseInt(itr.nextToken()); // to node
				if(from > MAX.get() || to > MAX.get()) // filter out max
					 continue;
				else
				{
					context.write(new IntWritable(to), new IntPair( from, to, 'T')); // emit To node as key, edges and 'T' tag as value
					context.write(new IntWritable(from), new IntPair( from, to, 'F')); // emit From node as key, edges and 'F' tag as values
				}
				
			}
		}
	}

	public static class JoinOne extends Reducer<IntWritable, IntPair, Text, Text> {

		@Override
		public void reduce(final IntWritable key, final Iterable<IntPair> values, final Context context) throws IOException, InterruptedException {
			
			ArrayList<IntPair> tolist = new ArrayList<IntPair>();
			ArrayList<IntPair> fromlist = new ArrayList<IntPair>();

			for (IntPair val : values)
			{
				if (val.getType() == 'T')
					tolist.add(new IntPair(val.getFirst(), val.getSecond(), val.getType())); // adding 'T' tag in to list
				else if (val.getType() == 'F')
					fromlist.add(new IntPair(val.getFirst(), val.getSecond(), val.getType())); // adding 'F' tag in from list
			}
			
			for (IntPair t_keys : tolist)
				for (IntPair f_keys : fromlist)
					if( t_keys.getFirst() != f_keys.getSecond()) //check for cycle (A-B-A)
						context.write(new Text(""+key), new Text(t_keys.getFirst() + "\t" +
										t_keys.getSecond() + "\t" + f_keys.getSecond()) ); // emit non-cycle path of len2
							
		}
	};
	
	public static class PathMapper extends Mapper<Object, Text, IntWritable, IntPair> { //Path mapper takes input of previous reducer
		private int from;
		private int to;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			while (itr.hasMoreTokens()) {
				itr.nextToken(); // first element is key skip
				from = Integer.parseInt(itr.nextToken()); // from is second element
				itr.nextToken(); // middle element is not required, skip
				to = Integer.parseInt(itr.nextToken()); // to element is the fourth element
				if(from > MAX.get() || to > MAX.get()) // filter out with max
					 continue;
				else
					context.write(new IntWritable(to), new IntPair( from, to, 'L')); // emit with to as key and values as From,to,'L' tag
			}
		}
	}
	
	public static class EdgesMapper extends Mapper<Object, Text, IntWritable, IntPair> { //mapper for input file (edges.csv) to find if the final edge in triangle exits
		private int from;
		private int to;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while (itr.hasMoreTokens()) {
				from = Integer.parseInt(itr.nextToken()); // from node
				to = Integer.parseInt(itr.nextToken()); // to node
				if(from > MAX.get() || to > MAX.get()) // filter
					continue;
				else
					context.write(new IntWritable(from), new IntPair( from, to, 'R')); //emit with from as key, value as edge with 'R' tag
			}
		}
	}
	
	public static class JoinTwo extends Reducer<IntWritable, IntPair, Text, Text> {

		@Override
		public void reduce(final IntWritable key, final Iterable<IntPair> values, final Context context) throws IOException, InterruptedException {
			
			ArrayList<IntPair> Llist = new ArrayList<IntPair>();
			ArrayList<IntPair> Rlist = new ArrayList<IntPair>();

			for (IntPair val : values)
			{
				if (val.getType() == 'L')
					Llist.add(new IntPair(val.getFirst(), val.getSecond(), val.getType())); // put 'L' tag in L list
				else if (val.getType() == 'R')
					Rlist.add(new IntPair(val.getFirst(), val.getSecond(), val.getType())); // put 'R' tag in R list
			}
			
			for (IntPair t_keys : Llist)
				for (IntPair f_keys : Rlist)
					if( t_keys.getSecond() == f_keys.getFirst() && t_keys.getFirst() == f_keys.getSecond()) // check for A-B in L tag there is B-A in 'R' tag
					{
						context.write(new Text(""+key), new Text(t_keys.getFirst() + "\t" + t_keys.getSecond() + "\t" + 
							f_keys.getFirst() +"\t" + f_keys.getSecond()) ); // emit the last edge of triangle
						context.getCounter(UpdateCounter.UPDATED).increment(1); // increment counter
					}
				
						
		}
	};
	

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		
		
		final Job job1 = Job.getInstance(conf, "Twitter Length 2");
		job1.setJarByClass(TwitterEdges.class);
		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
		job1.setMapperClass(TokenizerMapper.class);
		job1.setReducerClass(JoinOne.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntPair.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		
		final Job job2 = Job.getInstance(conf, "Twitter Length Cycle");
		job2.setJarByClass(TwitterEdges.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, PathMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, EdgesMapper.class);
		job2.setReducerClass(JoinTwo.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(IntPair.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		if (!job2.waitForCompletion(true)) {
			  System.exit(1);
			}
		long counter = job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();  
		System.out.println("***************************************\t" + counter);
		return 0;
		
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new TwitterEdges(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}