import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	private static Hashtable<String, Integer> state = new Hashtable<String, Integer>();
	private String targetWord; 

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String[] tokens = value.toString().toLowerCase().split("\\s+");

		if (tokens.length > 1) {
			for (int i = 1; i < tokens.length; i++) {

				if (!isTargetWord(tokens[i - 1], targetWord))
					continue;

				addWord(getRealWord(tokens[i]));

			}
		}
		
		//JLF: Printed out all consecutive words to the target words and the number of appearances for each word
		for (String word : state.keySet()) {
			output.collect(new Text(word), new IntWritable(state.get(word)));
		}

	}
	
	@Override
	public void configure(JobConf job){
		targetWord=job.get("word");
	}

	private String getRealWord(String word) {
		int position = 0;
		for (int j = 0; j < word.length(); j++)
			if (!Character.isLetter(word.charAt(j))) {
				position = j;
				break;
			}

		if (position > 1)
			word = word.substring(0, position);

		return word;
	}

	private boolean isTargetWord(String word, String targetWord) {

		if (word.length() > 0)
			if (Character.isLetter(word.charAt(0)))
				return word.equals(targetWord);

		int j = 0;
		for (j = 0; j < word.length(); j++)
			if (Character.isLetter(word.charAt(j)))
				break;

		return word.substring(j, word.length()).equals(targetWord);

	}

	private void addWord(String word) {

		state.put(word, state.get(word) != null ? state.get(word) + 1 : 1);

	}
}