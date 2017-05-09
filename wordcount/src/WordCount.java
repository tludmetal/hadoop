import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;


public class WordCount {

	public static void main(String[] args) throws Exception {
		
		/*
	     * Validate that thre arguments were passed from the command line.
	     */
	    if (args.length != 3) {
	      System.out.printf("Usage: Exercise1Driver <input dir> <output dir> <word>\n");
	      System.exit(-1);
	    }
	    
	    //FileUtils.deleteDirectory(new File(args[1]));
	    
	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    JobConf job = new JobConf(WordCount.class);
	    job.set("word", args[2]);

		job.setNumReduceTasks(1);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormat(ParagrapghInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		JobClient.runJob(job);
	}

}