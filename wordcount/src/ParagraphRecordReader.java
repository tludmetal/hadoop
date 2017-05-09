import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

public class ParagraphRecordReader implements RecordReader<LongWritable, Text> {

	private LineReader in;
	//private LongWritable key;
	//private Text value = new Text();
	private long start = 0;
	private long end = 0;
	private long pos = 0;
	private Text whiteSpace = new Text(" ");

	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	public LongWritable createKey(){
		return new LongWritable();
	}

	public Text createValue() {
		return new Text();
	}

	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public ParagraphRecordReader(Configuration job, 
			FileSplit genericSplit)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();

		FileSystem fs = file.getFileSystem(job);

		start = split.getStart();
		end = start + split.getLength();

		boolean skipFirstLine = false;
		FSDataInputStream filein = fs.open(split.getPath());

		if (start != 0) {
			skipFirstLine = true;
			--start;
			filein.seek(start);
		}
		in = new LineReader(filein, job);
		if (skipFirstLine) {
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	public boolean next(LongWritable key, Text value) throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		value.clear();

		int newSize = 0;
		boolean newline = false;

		Text v = new Text();
		while (!newline) {

			newSize = in.readLine(v);

			if (v.getLength() == 0)
				newline = true;

			value.append(v.getBytes(), 0, v.getLength());
			value.append(whiteSpace.getBytes(), 0, whiteSpace.getLength());
			pos += newSize;

			if (pos == end)
				break;
		}

		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}


	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		 return pos;
	}

}