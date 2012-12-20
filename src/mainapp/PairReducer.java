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

public class PairReducer  extends MapReduceBase
implements Reducer<TextPair,IntWritable,TextPair ,DoubleWritable>{
	private String firstMarginal = new String("");
	private double nMarginalCnt = 0;
	public void reduce(TextPair key,Iterator<IntWritable> values,
			OutputCollector<TextPair,DoubleWritable> output, Reporter reporter) throws IOException {
	    int sum = 0;
	    while (values.hasNext()) {
	      IntWritable value = (IntWritable) values.next();
	      sum += value.get(); // process value
	    }
	    if(key.getSecond().toString().compareToIgnoreCase("*")==0){//Means this is special key
	    	firstMarginal = key.getFirst().toString();
	    	nMarginalCnt = sum; 
	    }else{
	    	DoubleWritable freq = new DoubleWritable();
	    	double val =-1;
	    	if(key.getFirst().toString().compareToIgnoreCase(firstMarginal)==0){
	    		if(nMarginalCnt!=0){
	    			val = (sum/nMarginalCnt);
	    		}else{
	    			val=-2;
	    		}
	    	}
	    	freq.set(val);
	    	//Now collect the key value
	    	output.collect(key, freq);
	    }
	}
}