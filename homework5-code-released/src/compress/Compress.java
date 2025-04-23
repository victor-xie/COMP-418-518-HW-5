package compress;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import dsl.*;
import ecg.Data;

public class Compress {

	public static final int BLOCK_SIZE = 10;

	// Will return the query for delta-encoding
	public static Query<Integer, Integer> delta() {
		// TODO
		return new Query<>() {
			int prev = 0;

			@Override
			public void start(Sink<Integer> sink) {
				prev = 0;
			}

			@Override
			public void next(Integer item, Sink<Integer> sink) {
				int delta = item - prev;
				sink.next(delta);
				prev = item;
			}

			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};
	}

	// Will return the query for delta-decoding (inverse)
	public static Query<Integer, Integer> deltaInv() {
		// TODO
		return new Query<>() {
			int prev = 0;

			@Override
			public void start(Sink<Integer> sink) {
				prev = 0;
			}

			@Override
			public void next(Integer item, Sink<Integer> sink) {
				int value = item + prev;
				sink.next(value);
				prev = value;
			}

			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};
	}

	// Query for zigzag encoding
	public static Query<Integer, Integer> zigzag() {
		// TODO
		return Q.map(n -> (n << 1) ^ (n >> 31));

	}

	// Query for inverse of zigzag decoding (inverse)
	public static Query<Integer, Integer> zigzagInv() {
		// TODO
		return Q.map(z -> (z >>> 1) ^ -(z & 1));

	}

	// Query for packing blocks into byte-aligned compressed messages
	public static Query<Integer, Integer> pack() {
		// TODO
		return new Query<>() {
			Queue<Integer> block = new LinkedList<>();

			@Override
			public void start(Sink<Integer> sink) {
				block.clear();
			}

			@Override
			public void next(Integer item, Sink<Integer> sink) {
				block.add(item);
				if (block.size() == BLOCK_SIZE) {
					int max = 0;
					for (int val : block) max = Math.max(max, val);
					int bits = 32 - Integer.numberOfLeadingZeros(max);
					if (bits == 0) bits = 1;

					sink.next(bits); // header: bit width

					int acc = 0;
					int accLen = 0;
					for (int val : block) {
						acc |= (val << accLen);
						accLen += bits;
						while (accLen >= 8) {
							sink.next(acc & 0xFF);
							acc >>>= 8;
							accLen -= 8;
						}
					}
					if (accLen > 0) {
						sink.next(acc & 0xFF);
					}
					block.clear();
				}
			}

			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};

	}

	// Query for unpacking: compressed message -> recoverse elements that were
	// packed
	public static Query<Integer, Integer> unpack() {
		// TODO
		return new Query<>() {
			private int bitWidth = 0;
			private final LinkedList<Integer> buffer = new LinkedList<>();
			private int acc = 0;
			private int accLen = 0;
			private int valuesDecoded = 0;

			@Override
			public void start(Sink<Integer> sink) {
				bitWidth = 0;
				acc = 0;
				accLen = 0;
				valuesDecoded = 0;
				buffer.clear();
			}

			@Override
			public void next(Integer item, Sink<Integer> sink) {
				if (bitWidth == 0) {
					bitWidth = item;
					valuesDecoded = 0;
					acc = 0;
					accLen = 0;
				} else {
					acc |= (item << accLen);
					accLen += 8;
					while (accLen >= bitWidth && valuesDecoded < BLOCK_SIZE) {
						int val = acc & ((1 << bitWidth) - 1);
						sink.next(val);
						acc >>>= bitWidth;
						accLen -= bitWidth;
						valuesDecoded++;
					}
					if (valuesDecoded == BLOCK_SIZE) {
						bitWidth = 0;
						acc = 0;
						accLen = 0;
					}
				}
			}

			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};
	}

	// Query for compression
	public static Query<Integer, Integer> compress() {
		// TODO

		// HINT: PIPELINE OF QUERIES
		return Q.pipeline(delta(), zigzag(), pack());
	}

	// Query for decompression
	public static Query<Integer, Integer> decompress() {
		// TODO

		// HINT: PIPELINE OF QUERIES
		return Q.pipeline(unpack(), zigzagInv(), deltaInv());
	}

	public static void main(String[] args) {
		System.out.println("**********************************************");
		System.out.println("***** ToyDSL & Compression/Decompression *****");
		System.out.println("**********************************************");
		System.out.println();

		System.out.println("***** Compress *****");
		{
			// from range [0,2048) to [0,256)
			Query<Integer, Integer> q1 = Q.map(x -> x / 8);
			Query<Integer, Integer> q2 = compress();
			Query<Integer, Integer> q = Q.pipeline(q1, q2);
			Iterator<Integer> it = Data.ecgStream("100-all.csv");
			Q.execute(it, q, S.lastCount());
		}
		System.out.println();

		System.out.println("***** Compress & Decompress *****");
		{
			// from range [0,2048) to [0,256)
			Query<Integer, Integer> q1 = Q.map(x -> x / 8);
			Query<Integer, Integer> q2 = compress();
			Query<Integer, Integer> q3 = decompress();
			Query<Integer, Integer> q = Q.pipeline(q1, q2, q3);
			Iterator<Integer> it = Data.ecgStream("100-all.csv");
			Q.execute(it, q, S.lastCount());
		}
		System.out.println();
	}

}
