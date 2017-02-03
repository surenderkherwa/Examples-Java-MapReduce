package com.movielens.joincustomkey;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class PairedKeyComparator extends WritableComparator {
	
	private static final LongWritable.Comparator LONG_COMPARATOR = new LongWritable.Comparator();
	
	public PairedKeyComparator() {
		super(PairedKey.class);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof PairedKey && b instanceof PairedKey) {
			return ((PairedKey) a).compareTo(((PairedKey) b));
		}
		return super.compare(a, b);
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
			int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
			return LONG_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
