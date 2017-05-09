import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExtractLinksMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static Hashtable<Text, Integer> state = new Hashtable<Text, Integer>();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//JLF: There are only two values per line separated by tabulations
		String[] tokens = value.toString().toLowerCase().split("\\t+");

		//JLF:Put all values key(text), value(text)
		context.write(new Text(tokens[0]), new Text(tokens[1]));

	}

}