import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10Reducer extends
		Reducer<LongWritable, Text, Text, NullWritable> {

	private ArrayList<PageRankData> state = new ArrayList<PageRankData>();

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Text v = new Text();
		for (Text t : values)
			v.append(t.getBytes(), 0, t.getLength());

		String[] tokens = v.toString().toLowerCase().split("\\s+");

		PageRankData data = new PageRankData();
		data.setLinks(" " + tokens[0]);
		data.setPageRank(Double.parseDouble(tokens[1]));
		state.add(data);

	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		Collections.sort(state);
		for (int i = 0; i < state.size() && i < 10; i++)
			context.write(state.get(i).getLinks(), NullWritable.get());

	}
}