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

public class TestParaInputReducer extends MapReduceBase
implements Reducer<Text,Text,Text ,Text>{
	public void reduce(Text key,Iterator<Text> values,
			OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			Text value = (Text) values.next();
			output.collect(key, new Text(value));
		}

	}

}

