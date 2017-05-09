import org.apache.hadoop.io.Text;

public class SortedPair implements Comparable<SortedPair> {

	private Text word;
	private Integer count;

	public SortedPair(Text word, int count) {
		super();
		this.word = new Text(word);
		this.count = count;
	}

	@Override
	public int compareTo(SortedPair o) {
		return -1 * count.compareTo(o.count);
	}

	public Text getWord() {
		return word;
	}

	public Integer getCount() {
		return count;
	}

}
