package com.movielens.joincustomkey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class PairedKey implements WritableComparable<PairedKey> {

	private LongWritable key;
	private IntWritable index;

	public PairedKey() {
		set(new LongWritable(), new IntWritable());
	}

	public PairedKey(LongWritable key, IntWritable index) {
		set(key, index);
	}

	public PairedKey(long key, int index) {
		set(key, index);
	}

	public void set(long key, int index) {
		set(new LongWritable(key), new IntWritable(index));
	}

	public void set(LongWritable key, IntWritable index) {
		this.key = key;
		this.index = index;
	}
	
	public LongWritable getKey() {
		return key;
	}

	public IntWritable getIndex() {
		return index;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		index.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		index.readFields(in);
	}

	@Override
	public String toString() {
		return key + "\t" + index;
	}

	@Override
	public int compareTo(PairedKey pairedKey) {
		int cmp = key.compareTo(pairedKey.key);
		if (cmp != 0) {
			return cmp;
		}
		return index.compareTo(pairedKey.index);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((index == null) ? 0 : index.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof PairedKey) {
			PairedKey pairedKey = (PairedKey) o;
			return key.equals(pairedKey.key) && index.equals(pairedKey.index);
		}
		return false;
	}
	
}
