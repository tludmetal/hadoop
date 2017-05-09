import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class ParagrapghInputFormat extends TextInputFormat {
	
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit genericSplit, JobConf job,
			Reporter reporter) throws IOException {
		return new ParagraphRecordReader(job, (FileSplit) genericSplit);
	}
}