package hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



/*
 * 这个是二次排序的一个组合key和value的类
 * 
 * 
 * */
public class PairWritable  implements WritableComparable<PairWritable>{
	private String first;
	private int second;
	
	
	//这个是序列化的方法
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);//这里的循序要和下面readFields方法的顺序一致,否则反序列化的时候会报错
		out.writeInt(second);
		
		
		
	}
	//这个是反序列化
	public void readFields(DataInput in) throws IOException {
		this.first=in.readUTF();
		this.second=in.readInt();
		
		
	}

	
	
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + second;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairWritable other = (PairWritable) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	//对组合key 进行比较
	public int compareTo(PairWritable o) {
		int  comp=this.first.compareTo(o.getFirst());
		if(0!=comp){
			return comp;
		}
		return Integer.valueOf(this.second).compareTo(o.getSecond());
	}


	
	
	
	public PairWritable() {
		super();
	}

	public PairWritable(String first, int second) {
		this.set(first, second);
	}

	public void set(String first, int second){
		this.first = first;
		this.second = second;
		
	}
	
	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	
}
