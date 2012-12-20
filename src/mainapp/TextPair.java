package mainapp;

import java.io.DataOutput;
import java.io.DataInput;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair> {
	private Text first;
	private Text second;
	public TextPair() {
		set(new Text(), new Text());
	}
	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}
	public TextPair(Text first, Text second) {
		set(first, second);
	}
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	public Text getFirst() {
		return first;
	}
	public Text getSecond() {
		return second;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	@Override
	public int hashCode() {
		return first.hashCode();// * 163 + second.hashCode();//Compute hash onlt on the first string
	}
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}
	@Override
	public String toString() {
		String result = new String();
		result="<";
		result=result+first + "," + second+">";
		return result;
	}
	@Override
	public int compareTo(TextPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		cmp =second.compareTo(tp.second);
		if(cmp==0){
			return 0;
		}
		if(second.toString().equalsIgnoreCase("*")){//Means it is the special string that must arrived at reducer first
			return -1;
		}else{
			return second.compareTo(tp.second);
		}
	}
}
