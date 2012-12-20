package mainapp;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TestSortingReducer  extends MapReduceBase
implements Reducer<TextPair,IntWritable,TextPair ,IntWritable>{
	private String firstMarginal = new String("");
	private int nMarginalCnt = 0;
	public void reduce(TextPair key,Iterator<IntWritable> values,
			OutputCollector<TextPair,IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			IntWritable value = (IntWritable) values.next();
			sum += value.get(); // process value
		}
		IntWritable value = new IntWritable(sum);
		output.collect(key, value);
	}
}
