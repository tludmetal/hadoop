import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PageRankData implements Writable, WritableComparable<PageRankData> {

	private Text links = new Text("");
	private DoubleWritable pagerank = new DoubleWritable();
	private BooleanWritable isDouble = new BooleanWritable();

	public Text getLinks() {
		return links;
	}

	public void setLinks(String links) {
		this.links = new Text(links);
	}

	public DoubleWritable getPr() {
		return pagerank;
	}

	public void setPageRank(double pr) {
		this.pagerank = new DoubleWritable(pr);
		this.isDouble.set(true);
	}

	public boolean IsDouble() {
		return this.isDouble.get();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		links.write(out);
		pagerank.write(out);
		isDouble.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		links.readFields(in);
		pagerank.readFields(in);
		isDouble.readFields(in);
	}

	@Override
	public int compareTo(PageRankData o) {
		return -1 * this.pagerank.compareTo(o.pagerank);
	}

}
