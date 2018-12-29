package tc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
	private final static IntWritable MAX = new IntWritable(58000);
	
	
	public static enum UpdateCounter {
		  UPDATED;
		}


	public static class FirstMap extends Mapper<Object, Text, Text, Text> {
		private int from;
		private int to;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while (itr.hasMoreTokens()) {
				from = Integer.parseInt(itr.nextToken());
				to = Integer.parseInt(itr.nextToken());
				if(from > MAX.get() || to > MAX.get())
					 continue;
				else
				{
					
					context.write(new Text(""+from), new Text(""+to));
				}
				
			}
		}
	}

	public static class FirstReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			for (Text val : values)
				context.write(key, val);					
		}
	};
	
	public static class SecondMap extends Mapper<Object, Text, Text, Text> {
		private int from;
		private int to;
		private HashMap<Integer, ArrayList<Integer>> map = new HashMap<Integer, ArrayList<Integer>>();
		
		@Override
		public void setup(Context context) throws IOException,InterruptedException {
			File cachedFile = new File("cEdges");
	        FileInputStream fileInputStream = new FileInputStream(cachedFile);
	        BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
	        String thisLine;
			while ((thisLine = br.readLine()) != null)
			{
				String[] s = thisLine.split(",");
				from = Integer.parseInt(s[0]);
				to = Integer.parseInt(s[1]);
				map.putIfAbsent(from, new ArrayList<Integer>());
				map.get(from).add(to);
			}
	            
	            
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] s = value.toString().split(",");
			from = Integer.parseInt(s[0]);
			to = Integer.parseInt(s[1]);
			ArrayList<Integer> goingTo = map.getOrDefault(to, new ArrayList<Integer>());
			for( Integer val : goingTo)
			{
				if(val != from)
				{
					ArrayList<Integer> goingBack = map.getOrDefault(val, new ArrayList<Integer>());
					for (Integer isfrom : goingBack)
					{
						if( isfrom == from)
						{
							context.getCounter(UpdateCounter.UPDATED).increment(1);
						}
					}
					
				}
			}
			
		}
	}

	

	

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		
		
		final Job job1 = Job.getInstance(conf, "Twitter Length 2");
		job1.setJarByClass(TwitterEdges.class);
		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", ",");
		job1.setMapperClass(FirstMap.class);
		job1.setNumReduceTasks(1);
		job1.setReducerClass(FirstReduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		
		final Job job2 = Job.getInstance(conf, "Twitter Length Cycle");
		job2.setJarByClass(TwitterEdges.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
		
		job2.addCacheFile(new URI(args[1]+"/part-r-00000"+"#cEdges"));
		job2.setMapperClass(SecondMap.class);
		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
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