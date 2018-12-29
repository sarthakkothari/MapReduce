package km;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class KMeans extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(KMeans.class);
	
	
	public static enum UpdateCounter {
		  UPDATED
		}
	
	public static class FollowerMapper extends Mapper<Object, Text, IntWritable, IntWritable> { // Mapper for creating followers
		private int to;
		private int from;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ","); // parse the file
			while (itr.hasMoreTokens()) {
				from = Intger.parseInt(itr.nextToken()); // from node
				to = Integer.parseInt(itr.nextToken()); // to node
				context.write(new IntWritable(to), new IntWritable(1)); // Increase the follower count by 1
				context.write(new IntWritable(from), new IntWritable(0)); // Takes care of Users with 0 followers
			}
		}
	}

	public static class FollowerReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> { //Same Combiner / Reducer job.

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable val : values)  // sum all the values in the list
				sum += val.get();
			context.write(key, new IntWritable(sum)); // print the count of number of followers
		}
	};
	
	public static class KMeansExpectation extends Mapper<Object, Text, DoubleWritable, DoubleWritable> { // mapper for expectation phase of KMeans
		private int user;
		private int followers;
		ArrayList<Double> centerList = new ArrayList<Double>();
		
		protected void setup(Context context) throws IOException, InterruptedException {  
				String[] s = context.getConfiguration().get("Centers").split(" "); // get centers from driver
				for (String temp : s) 
					centerList.add(Double.parseDouble(temp)); // store centers in the list
		}		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ","); // parse the file
			while (itr.hasMoreTokens()) {
				user= Integer.parseInt(itr.nextToken()); // userID node
				followers = Integer.parseInt(itr.nextToken()); // follower count for user ID
			}
			double min = Double.MAX_VALUE;
			double cluster = 0;
			for(double val : centerList) // for all centeroids
			{
				double d = Math.abs(val-followers);
				if (d < min) // find the closest centeroid
				{
					min = d;
					cluster = val;
				}
			}
			context.write(new DoubleWritable(cluster), new DoubleWritable(followers)); // pass the centeroid userID belongs to and the followerCount
		}
		
		@Override
		public void cleanup(final Context context) throws IOException, InterruptedException { // take care of centeroids with 0 userIDs
			super.cleanup(context);
			for (double center : centerList)
				context.write(new DoubleWritable(center), new DoubleWritable(-1.0)); // pass -1 so that the centeroid isn't lost.
		}
		
	}
	
	public static class KMeansMaximization extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable> { // reducer for Maximization phase of KMeans

		@Override
		public void reduce(final DoubleWritable key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
			double totalf=0, totalc=0, sse=0, newc=0;
			for(DoubleWritable val: values)
			{
				if (val.get() != -1.0)
				{
					totalf += val.get(); // sum of follower in the cluster
					totalc +=1; // total users in the cluster
					sse += Math.pow(key.get()-val.get(), 2); // calculate sse
				}
			}
			if (totalf == 0) // check if any user belongs to cluster
				newc = key.get();
			else
				newc = totalf / totalc; // update the centeroid as average of all the follower counts in the cluster
			if (Math.abs(newc - key.get()) > 10E-3)
					context.getCounter(KMeans.UpdateCounter.UPDATED).increment(1); // if there is a change in centeroid increment Global counter
			context.write(new DoubleWritable(newc), new DoubleWritable(sse));	// write new cluster and sse of that cluster
		}
	};
	
	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		int k = Integer.parseInt(args[2]);
		int min=1;
		int max=564512;
		Random r = new Random(); 
		String s = "";
		for (int i=0; i<k; i++)
			s += ""+ (min + r.nextInt((max - min) + 1))+" "; // randomly selecting k-sources
		//s = "0.0 141128.0 282256.0 423384.0"; // Good Cluster
		//s="1.0 2.0 3.0 4.0"; // bad cluster
		final Job job1 = Job.getInstance(conf, "K Means Follower Count");
		job1.setJarByClass(KMeans.class);
		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", ",");
		job1.setMapperClass(FollowerMapper.class);
		job1.setCombinerClass(FollowerReducer.class);
		job1.setReducerClass(FollowerReducer.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"0"));
		job1.waitForCompletion(true);
		// first job for counting followers

		long counter = 0;
		int i = 0;
		do {
			final Job job2 = Job.getInstance(conf, "K Means EM-Job :"+i);
			job2.setJarByClass(KMeans.class);
			final Configuration jobConf2 = job2.getConfiguration();
			jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
			job2.getConfiguration().set("Centers",s);
			job2.setMapperClass(KMeansExpectation.class);
			job2.setReducerClass(KMeansMaximization.class);
			job2.setMapOutputKeyClass(DoubleWritable.class);
			job2.setMapOutputValueClass(DoubleWritable.class);
			job2.setOutputKeyClass(DoubleWritable.class);
			job2.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job2, new Path(args[1]+ "0"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_centers_" +(i+1)));
			// set mappers, reducers, input and output paths
			i +=1;
			if (!job2.waitForCompletion(true)) {
				  System.exit(1);
				}
			counter = job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();   
			double sse=0;
			s = "";
			Path p = new Path(args[1].replace("s3://sarthakkotharitest>", "")+"_centers_" +i); // read the path that stores centeroid
			FileSystem fs = FileSystem.get(URI.create("s3://sarthakkotharitest"), job2.getConfiguration()); // make Hadoop filesystem object
			FileStatus[] fileList = fs.listStatus(p); // List files
			for (FileStatus temp : fileList) {
				String fileName = temp.getPath().toString(); // get the file path
            		if (fileName.contains("part") && !fileName.contains(".crc")) { // check if it is a file written by reducer
            			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(fileName)))); // create input stream
                		String thisLine;
                		while ((thisLine = br.readLine()) != null) {
                			String[] st = thisLine.split(","); 
                			sse += Double.parseDouble(st[1]); // calculate sse for the current iteration
                			s +=  "" + st[0] + " "; // make the centeroid string to pass to job in next iteration
                		}
            		}
            	}
			System.out.println("***************************************\t" + i +"\t"+ counter + "\t"+ s + "\t"+ sse );// print SSE
		}while(counter != 0 && i <= 10); // while centeriods have been update or iteration is less than 11 we keep running EM iteration of KMeans
		return 0;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <No.of Clusters>");
		}

		try {
			ToolRunner.run(new KMeans(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}