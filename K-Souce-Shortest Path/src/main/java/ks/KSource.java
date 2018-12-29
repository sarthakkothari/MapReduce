package ks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.stream.IntStream;
import ks.VertexObj;

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

import com.google.gson.Gson;

public class KSource extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(KSource.class);
	
	
	public static enum UpdateCounter {
		  UPDATED;
		}

	public static class AdjMapper extends Mapper<Object, Text, IntWritable, IntWritable> { // Mapper for creating adjacency list
		private int from;
		private int to;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ","); // parse the file
			while (itr.hasMoreTokens()) {
				from = Integer.parseInt(itr.nextToken()); // from node
				to = Integer.parseInt(itr.nextToken()); // to node
				context.write(new IntWritable(from), new IntWritable(to)); // Pass from as node ID and edge it is going to for Adjlist
				context.write(new IntWritable(to), new IntWritable(0)); // Takes care of Dangling nodes
			}
		}
	}

	public static class AdjReducer extends Reducer<IntWritable, IntWritable, VertexObj, Text> {

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			
			ArrayList<Integer> adjList = new ArrayList<Integer>(); // adjacency list
			VertexObj ob;
			String[] s = context.getConfiguration().get("Sources").split(" "); // extract the source IDs
			ArrayList<Integer> keylist = new ArrayList<Integer>();
			for (String temp : s) 
				keylist.add(Integer.parseInt(temp)); // store them in a key list
			
			for (IntWritable val : values) // generate adjacency list for that key.
				if (val.get() != 0)
					adjList.add(val.get()); 

			if (keylist.contains(key.get())) // check if its source, make it active
				ob = new VertexObj(key.get(), adjList, true);
			else
				ob = new VertexObj(key.get(), adjList, false); 
			context.write(ob, null); // write to context
		}
	};
	
	public static class KSourceMapper extends Mapper<Object, Text, IntWritable, VertexObj> { // mapper for finding K source
		private Gson gson = new Gson();
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			
			VertexObj val = gson.fromJson(value.toString(), VertexObj.class); // read from the output of previous iteration
			String[] s = context.getConfiguration().get("Sources").split(" "); // get source from driver
			ArrayList<Integer> keylist = new ArrayList<Integer>();
			
			for (String temp : s) 
				keylist.add(Integer.parseInt(temp)); // store source in the list
			
			
			if (keylist.contains(val.getId()))
				val.getMap().putIfAbsent(val.getId(), 0); // for all sources, set the map
			
			
			val.setObject(true); //set is object true
			context.write(new IntWritable(val.getId()), val); // pass as object
			
			
			if (val.getStatus()) // if object is active
				for (Integer vertex: val.getAdjList()) // check adj list
				{
					HashMap<Integer, Integer> map = new HashMap(val.getMap()); // get the map for disnace of current object
					for (Integer k : map.keySet())
						map.put(k, map.get(k)+1); // increment the distance by 1
					context.write(new IntWritable(vertex), new VertexObj(vertex, map)); // pass adjacent elements with hashmap as with distances.
				}
		}
	}
	
	public static class KSourceReducer extends Reducer<IntWritable, VertexObj, VertexObj, Text> {

		@Override
		public void reduce(final IntWritable key, final Iterable<VertexObj> values, final Context context) throws IOException, InterruptedException {
			
			HashMap<Integer,Integer> map = new HashMap<Integer, Integer>();
			
			String[] s = context.getConfiguration().get("Sources").split(" ");  // get source from driver
			ArrayList<Integer> keylist = new ArrayList<Integer>();
			
			for (String temp : s)
				keylist.add(Integer.parseInt(temp)); // store source in the list
			
	
			for(int k : keylist)
				map.putIfAbsent(k, Integer.MAX_VALUE); // initialize a hashmap as max for all k-sources.
			
				
			VertexObj M = null;
			for (VertexObj ob : values)
			{
				if (ob.isObject() ) {
					M = new VertexObj(ob);
					M.setStatus(false); // if you get the object set active as false 
				}
				else
				{
					for (int key_1 : ob.getMap().keySet())
					{
						if (map.get(key_1) > ob.getMap().get(key_1)) // compare distance
							map.put(key_1, ob.getMap().get(key_1)); // assign if smaller distance
					}
				}
			}
			
			for (Integer val : map.keySet())
			{
				if (! M.getMap().containsKey(val) && map.get(val) != Integer.MAX_VALUE) // if the k-source has been processed and value of map is not Infinity
				{
					M.getMap().put(val, map.get(val)); // put the value in the map
					M.setStatus(true); // make that node as active
					context.getCounter(UpdateCounter.UPDATED).increment(1);
				}
				if (M.getMap().containsKey(val))
				{
					if (map.get(val) < M.getMap().get(val))
					{
						M.getMap().put(val, map.get(val)); // if map value is less than object value (found a shorter path)
						M.setStatus(true); // make node as active
						context.getCounter(UpdateCounter.UPDATED).increment(1);
					}
				}
					
			}
			
			context.write(M, null); // write object to context
		}
	};
	
	public static class MaxHopMapper extends Mapper<Object, Text, IntWritable, IntWritable> { // mapper for finding max distance
		private Gson gson = new Gson();
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			
			VertexObj val = gson.fromJson(value.toString(), VertexObj.class); // read from the output of previous iteration
			HashMap<Integer, Integer> map = val.getMap();
			for (Integer k : map.keySet())
				context.write(new IntWritable(k), new IntWritable(map.get(k))); // emit the entire hashmap as key value where keys are our souces
		}
	}
	public static class MaxHopReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> { // reducer for finding max

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int max = 0;
			for( IntWritable val : values) // find max from list of values
				if (val.get() > max)
					max = val.get();
			context.write(key, new IntWritable(max)); // write max
		}
	};
	
		
	

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		int k = Integer.parseInt(args[2]);
		int min=1;
		int max=110000;
		// job1 for creating objects and setting sources as active
		final Job job1 = Job.getInstance(conf, "K Source");
		job1.setJarByClass(KSource.class);
		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

		Random r = new Random(); 
		String s = "";
		for (int i=0; i<k; i++)
			s += ""+ (min + r.nextInt((max - min) + 1))+" "; // randomly selecting k-sources
		System.out.print("\n\n\n****************" + s);
		job1.getConfiguration().set("Sources",s); // passing the sources from driver under alias sources.
		job1.setMapperClass(AdjMapper.class);
		job1.setReducerClass(AdjReducer.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(VertexObj.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"0"));
		job1.waitForCompletion(true);
		
		//job2 for finding k source if there is no change in counter, algorithm as converged.
		long counter = 0;
		int i = 0;
		do {
			final Job job2 = Job.getInstance(conf, "K source Job 2");
			job2.setJarByClass(KSource.class);
			final Configuration jobConf2 = job2.getConfiguration();
			jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
			job2.getConfiguration().set("Sources",s);
			
			
			job2.setMapperClass(KSourceMapper.class);
			job2.setReducerClass(KSourceReducer.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(VertexObj.class);
			job2.setOutputKeyClass(VertexObj.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(args[1]+ i));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + (i+1)));
			i +=1;
			if (!job2.waitForCompletion(true)) {
				  System.exit(1);
				}
			counter = job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();  
			System.out.println("***************************************\t" + i);
		}while(counter != 0);
		
		// job3 for finding max distances
		
		final Job job3 = Job.getInstance(conf, "K Source Job 3");
		job3.setJarByClass(KSource.class);
		final Configuration jobConf3 = job3.getConfiguration();
		jobConf3.set("mapreduce.output.textoutputformat.separator", "\t");
		job3.getConfiguration().set("Sources",s);
		job3.setMapperClass(MaxHopMapper.class);
		job3.setReducerClass(MaxHopReducer.class);
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass(VertexObj.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(args[1] + i));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+(i+1)));
		job3.waitForCompletion(true);
		
		
		
		
		return 0;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <No.ofKsource>");
		}

		try {
			ToolRunner.run(new KSource(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}