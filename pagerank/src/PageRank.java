import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		Job job = new Job(configuration);
		job.setJarByClass(PageRank.class);
		job.setMapperClass(ExtractLinksMapper.class);
		job.setReducerClass(ExtractLinksReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.waitForCompletion(true);

		String input = args[1];
		String output = "";
		for (int i = 0; i < 50; i++) {

			output = args[2]+"/"+i;
			FileUtils.deleteDirectory(new File(output));

			job = new Job();
			job.setJarByClass(PageRank.class);
			job.setMapperClass(CalculatePRMapper.class);
			job.setReducerClass(CalculatePRReducer.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(PageRankData.class);

			job.waitForCompletion(true);

			
			input = output;
			

		}

		job = new Job();
		job.setJarByClass(PageRank.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Top10Reducer.class);
		
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(output));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File(args[1]));
		FileUtils.deleteDirectory(new File(args[2]));

		System.exit(ToolRunner.run(new PageRank(), args));

	}
}