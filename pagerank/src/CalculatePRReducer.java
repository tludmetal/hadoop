import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculatePRReducer extends Reducer<Text, PageRankData, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<PageRankData> values, Context context)
			throws IOException, InterruptedException {

		Double pr = 0.0;
		String links = "";

		for (PageRankData v : values)
			if (v.IsDouble())
				pr += v.getPr().get();
			else
				links += v.getLinks().toString();

		//JLF:Damping factor 0.85
		pr = pr * 0.85;

		//JLF pr+(1-d) with damping factor 0.85
		pr += 0.15;
		
		
		//JLF: First object contains page rank, plus links already retrieved
		context.write(key, new Text(pr + links));

	}

}