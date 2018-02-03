import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public final static String STOPWORD_FILENAME = "stopwords.txt";
		private static final int BUFFER_SIZE = 4096;

		public static Map<String, Boolean> getStopWords() {
			byte[] buffer = new byte[BUFFER_SIZE];
			int bytesRead = 0;
			StringBuilder sb = new StringBuilder("");
			InputStream in = null;
			Map<String, Boolean> stopwords = new HashMap<String, Boolean>();
			try {
				in = new BufferedInputStream(WordCount.class.getClassLoader().getResourceAsStream(STOPWORD_FILENAME));
				while ((bytesRead = in.read(buffer)) != -1) {
					sb.append(new String(buffer, 0, bytesRead));
				}
				String stopwordString = sb.toString();

				String[] stopwordArray = stopwordString.split(",");
				for (String stopword : stopwordArray) {
					if (stopword.length() > 4) {
						stopwords.put(stopword, true);
					}
				}
				in.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
			return stopwords;
		}

		private final static Map<String, Boolean> STOP_WORDS = getStopWords();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String nextToken = "";
			while (itr.hasMoreTokens()) {
				nextToken = itr.nextToken().toLowerCase();
				nextToken = nextToken.replaceAll("[^a-zA-Z0-9]", "");
				if (!(nextToken.length() < 5 || STOP_WORDS.containsKey(nextToken))) {
					word.set(nextToken);
					context.write(word, one);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}