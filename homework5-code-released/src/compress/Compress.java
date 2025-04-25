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
				// Accumulate one more value into our current BLOCK_SIZE chunk.
				block.add(item);
				if (block.size() == BLOCK_SIZE) {
					// Compute the bit-width needed to represent every value in this block
					// by OR bit masking them together and counting how many bits that result needs
					// because it gives the position of the highest-position 1 in their bitstrings
					int farthestOne = 0;
					for (int v : block)
						farthestOne |= v;
					// now has 1s in every bit-position that any v had set
					int bitWidth = 32 - Integer.numberOfLeadingZeros(farthestOne);
					// Bitstring 0 still has width 1
					if (bitWidth == 0)
						bitWidth = 1;

					// Assuming 8-bit input, largest zigzag-encoded value is 510
					// which has a bitwidth of 9, which can be stored in 4 tsbi
					int header = (bitWidth) & 0xF;

					// Accumulate the bits from the truncated bitstrings and
					// concatenate. Whenever an 8-bit (1 byte) chunk is full
					// send it to the sink as an Integer
					long bitQueue = header; // Initially we start with just the bitwidth header
					int currBits = 4; // the length starts off as 3, the length of the header

					// Bitmask extracts the non-truncated bits of each element in the block
					long truncateMask = (1L << bitWidth) - 1;

					// For each of the BLOCK_SIZE (10) values, concatenate their truncated bits
					for (int v : block) {
						// bitshift left by bitWidth, then OR in the new bits:
						// v & mask gets you the bits you want to send out
						// then we OR to concatenate
						bitQueue = (bitQueue << bitWidth) | (v & truncateMask);
						currBits += bitWidth;

						// Enough bits for a byte, so send it out
						while (currBits >= 8) {
							int shift = currBits - 8;
							// Get the first byte in order
							sink.next((int) ((bitQueue >> shift) & 0xFF));

							// Remove the sent bits from the length tracker and the bit queue
							currBits -= 8;
							bitQueue &= (1L << currBits) - 1;
						}
					}

					// Flush leftover bits with padding so the msg is byte-aligned
					if (currBits > 0) {
						sink.next((int) ((bitQueue << (8 - currBits)) & 0xFF));
					}
					block.clear();
				}
			}

			@Override
			public void end(Sink<Integer> sink) {
				// INSTRUCTOR ASSUMPTION: input is of length guaranteed multiple of BLOCK_SIZE
				// Thus, no extra on ending
				sink.end();
			}
		};
	}

	// Query for unpacking: compressed message -> recovers elements that were
	// packed
	public static Query<Integer, Integer> unpack() {
		// TODO
		return new Query<>() {
			// Unlike pack(), we needed these values to persist across calls to next()
			// because they need to re-accumulate the compressed bytes to rebuild each block
			private int bitWidth = 0; // block width
			private long bitQueue = 0; // bits waiting to be de-compressed
			private int currBits = 0; // number of total bits in the queue
			private int countInBlock = 0; // how many of the current block have been decompressed

			@Override
			public void start(Sink<Integer> sink) {
				bitWidth = 0;
				bitQueue = 0;
				currBits = 0;
				countInBlock = 0;
			}

			@Override
			public void next(Integer item, Sink<Integer> sink) {
				// Add new byte to bitQeuue
				int b = item & 0xFF;
				bitQueue = (bitQueue << 8) | b;
				currBits += 8;

				// If no header yet (0 is impossible in this scheme), consume 4 bits
				if (bitWidth == 0) {
					if (currBits < 4) {
						return;
					}
					int header = (int) ((bitQueue >> (currBits - 4)) & 0xF);
					bitWidth = header;
					currBits -= 4;
					bitQueue &= (1L << currBits) - 1;
				}

				// Extract BLOCK_SIZE * bitWidth bits
				while (countInBlock < BLOCK_SIZE && currBits >= bitWidth) {
					int shift = currBits - bitWidth;
					int v = (int) ((bitQueue >> shift) & ((1 << bitWidth) - 1));
					sink.next(v); // Value is decompressed, send off
					currBits -= bitWidth;
					bitQueue &= (1L << currBits) - 1;
					countInBlock++;
				}

				// Reset for next block
				if (countInBlock == BLOCK_SIZE) {
					bitWidth = 0;
					countInBlock = 0;
					bitQueue = 0;
					currBits = 0;
				}
			}

			@Override
			public void end(Sink<Integer> sink) {
				// INSTRUCTOR ASSUMPTION: input is of length guaranteed multiple of BLOCK_SIZE
				// Thus, no extra on ending
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
