import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, NullWritable> {

	private ArrayList<SortedPair> list = new ArrayList<SortedPair>();
	
	private OutputCollector<Text, NullWritable> op;

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
		int v = 0;
		if (op==null)
			op=output;
		while (values.hasNext()) 
			v += values.next().get();

		//JLF: Add all words plus number appearances
		list.add(new SortedPair(key, v));
	}
	

	public void close() throws IOException {
		Collections.sort(list);
		//JLF: Print ten first results of an order list by number of appearances
		for (int i = 0; i < list.size() && i < 10; i++) {
			System.out.println(list.get(i).getWord()+" "+ i);
			op.collect(list.get(i).getWord(), NullWritable.get());
		}

	}

}