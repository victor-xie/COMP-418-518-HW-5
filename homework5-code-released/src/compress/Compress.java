package compress;

import dsl.*;
import ecg.Data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
				// First item will be original
				// All subsequent items will be diffs
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
				// First value will be the same
				// Others will be reconstructed by sum
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
			private final List<Integer> block = new ArrayList<>(BLOCK_SIZE);
		
			@Override
			public void start(Sink<Integer> sink) {
			  block.clear();
			}
		
			@Override
			public void next(Integer item, Sink<Integer> sink) {
			  block.add(item);
			  if (block.size() == BLOCK_SIZE) {
				// 1) bitWidth = max bits needed among block
				int orAll = 0;
				for (int v : block) orAll |= v;
				int bitWidth = 32 - Integer.numberOfLeadingZeros(orAll);
				if (bitWidth == 0) bitWidth = 1;
				// we'll store (bitWidth−1) in 3 bits [0..7]  
		
				// 2) build a big‐endian bit accumulator
				long acc = 0;
				int accLen = 0;
		
				// 2a) push header = (bitWidth−1) in 3 bits
				int header = (bitWidth - 1) & 0x7;
				acc = header;
				accLen = 3;
		
				// 2b) push each value’s bottom bitWidth bits
				long mask = (1L << bitWidth) - 1;
				for (int v : block) {
				  acc = (acc << bitWidth) | (v & mask);
				  accLen += bitWidth;
				  // emit top bytes as soon as we have ≥8 bits
				  while (accLen >= 8) {
					int shift = accLen - 8;
					sink.next((int)((acc >> shift) & 0xFF));
					accLen -= 8;
					acc &= (1L << accLen) - 1;
				  }
				}
		
				// 2c) flush remaining (pad low bits with 0)
				if (accLen > 0) {
				  sink.next((int)((acc << (8 - accLen)) & 0xFF));
				}
		
				block.clear();
			  }
			}
		
			@Override
			public void end(Sink<Integer> sink) {
			  // guaranteed multiple of BLOCK_SIZE so nothing extra
			  sink.end();
			}
		  };
	}

	// Query for unpacking: compressed message -> recovers elements that were
	// packed
	public static Query<Integer, Integer> unpack() {
		// TODO
		return new Query<>() {
			private int bitWidth = 0;
			private long acc = 0;
			private int accLen = 0;
			private int countInBlock = 0;
		
			@Override
			public void start(Sink<Integer> sink) {
			  bitWidth = 0;
			  acc = 0;
			  accLen = 0;
			  countInBlock = 0;
			}
		
			@Override
			public void next(Integer item, Sink<Integer> sink) {
			  int b = item & 0xFF;
			  // 1) append new byte
			  acc = (acc << 8) | b;
			  accLen += 8;
		
			  // 2) if we don't yet know bitWidth, read 3 header bits
			  if (bitWidth == 0) {
				if (accLen < 3) return;        // need more bytes
				int header = (int)((acc >> (accLen - 3)) & 0x7);
				bitWidth = header + 1;
				accLen -= 3;
				acc &= (1L << accLen) - 1;
			  }
		
			  // 3) now extract exactly BLOCK_SIZE values
			  while (countInBlock < BLOCK_SIZE && accLen >= bitWidth) {
				int shift = accLen - bitWidth;
				int v = (int)((acc >> shift) & ((1 << bitWidth) - 1));
				sink.next(v);
				accLen -= bitWidth;
				acc &= (1L << accLen) - 1;
				countInBlock++;
			  }
		
			  // 4) once we hit BLOCK_SIZE, reset for next header
			  if (countInBlock == BLOCK_SIZE) {
				bitWidth = 0;
				countInBlock = 0;
				// need to flush the accumulator:
				acc = 0;
				accLen = 0;
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
