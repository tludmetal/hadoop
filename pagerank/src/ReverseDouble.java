import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ReverseDouble implements Writable,
		WritableComparable<ReverseDouble> {

	DoubleWritable d = new DoubleWritable();

	public ReverseDouble() {
	}

	public ReverseDouble(double d) {
		this.d = new DoubleWritable(d);

	}

	@Override
	public int compareTo(ReverseDouble rd) {
		return -1 * d.compareTo(rd.d);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		d.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		d.write(out);
	}

}
