import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculatePRMapper extends Mapper<LongWritable, Text, Text, PageRankData> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//JLF:Splt each line by all values related to the key using white space
		String[] tokens = value.toString().toLowerCase().split("\\s+");

		//JLF: Total outlinks will be all values minus first value and second value which it represents pagerank
		double outLinks = tokens.length - 2;
		String links = "";

		for (int i = 2; i < tokens.length; i++) {
			PageRankData data = new PageRankData();
			data.setPageRank(Double.parseDouble(tokens[1]) / outLinks);
			context.write(new Text(tokens[i]), data);
			links += " " + tokens[i];
		}

		PageRankData data = new PageRankData();
		data.setLinks(links);
		context.write(new Text(tokens[0]), data);

	}

}