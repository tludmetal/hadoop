import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExtractLinksReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		
		//JLF:First value contains page rank, starts 1
		String value = "1";

		for (Text v : values)
			value += " " + v.toString();

		//JLF: For each node retrieves and ordered input with pagerank plus all links related separated by white space
		context.write(key, new Text(value));

	}

}