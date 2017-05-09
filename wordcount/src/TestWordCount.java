import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, NullWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, NullWritable> mapReduceDriver;
	
	@Before
	public void setUp()
	{
	    WordCountMapper mapper = new WordCountMapper();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    Configuration configuration = new Configuration();
	    configuration.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization," 
	            + "org.apache.hadoop.io.serializer.WritableSerialization");
	    configuration.set("word", "for");
	    mapDriver.setConfiguration(configuration);
	    mapDriver.setMapper(mapper);
	 
	    WordCountReducer reducer = new WordCountReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, NullWritable>();
	    reduceDriver.setReducer(reducer);
	 
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, NullWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() throws IOException
	{
	    mapDriver.withInput(new LongWritable(1), new Text("my for orange orange my for apple for orange"));
	    mapDriver.withOutput(new Text("apple"), new IntWritable(1));
	    mapDriver.withOutput(new Text("orange"), new IntWritable(2));
	    mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException
	{
	    List values = new ArrayList();
	    values.add(new IntWritable(5));
	    values.add(new IntWritable(1));
	    List values2 = new ArrayList();
	    values2.add(new IntWritable(2));
	    values2.add(new IntWritable(2));
	    reduceDriver.withInput(new Text("orange"), values);
	    reduceDriver.withInput(new Text("apple"), values2);
	    reduceDriver.withOutput(new Text("apple"), NullWritable.get());
	    reduceDriver.withOutput(new Text("orange"), NullWritable.get());
	    reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws IOException
	{
	    mapReduceDriver.addInput(new LongWritable(1), new Text("orange orange apple"));
	    mapReduceDriver.addOutput(new Text("orange"), NullWritable.get());
	    mapReduceDriver.addOutput(new Text("apple"), NullWritable.get());
	    mapReduceDriver.runTest();
	}

}
