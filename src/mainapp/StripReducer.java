package mainapp;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.MapWritable;

public class StripReducer extends MapReduceBase
implements Reducer<Text,MapWritable,TextPair ,DoubleWritable>{
	public void reduce(Text key, Iterator<MapWritable> values,
			OutputCollector<TextPair,DoubleWritable> output, Reporter reporter) throws IOException {
		MapWritable aggreGateMap = new MapWritable();
		double nTotalCount = 0;
		while(values.hasNext()){
			MapWritable stripeMap=(MapWritable)values.next();
			Set stripeSet = stripeMap.entrySet();
			Iterator iter = stripeSet.iterator();
			while(iter.hasNext()){
				Entry<Text,IntWritable>stripeEntry =(Entry<Text,IntWritable>)iter.next();
				Text stripeKey = stripeEntry.getKey();
				IntWritable tmp = (IntWritable)aggreGateMap.get(stripeKey);
				if(tmp==null){
					tmp = new IntWritable(0);
					aggreGateMap.put(stripeKey,tmp);
				}
				nTotalCount+=stripeEntry.getValue().get();
				tmp.set(tmp.get()+stripeEntry.getValue().get());
				aggreGateMap.put(stripeKey,tmp);
			}
		}
		Set aggMapSet = aggreGateMap.entrySet();
		Iterator iter = aggMapSet.iterator();
		while(iter.hasNext()){
			Entry<Text,IntWritable>AggEntry =(Entry)iter.next();
			double val = AggEntry.getValue().get()/nTotalCount;
			DoubleWritable freq = new DoubleWritable(val);
			/*aggreGateMap.put(AggEntry.getKey(), freq);*/
			TextPair keyPair = new TextPair(key,AggEntry.getKey());
			output.collect(keyPair,freq);
		}

	}
}
