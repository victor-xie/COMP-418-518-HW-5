package ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dsl.Query;
import dsl.Sink;
import utils.Pair;
import utils.functions.Func2;

// A streaming implementation of the "group by" (and aggregate) operator.
//
// The input consists of one channel that carries key-value pairs of the
// form (k, a) where k \in K and a \in A.
// For every key k, we perform a separate aggregation in the style of fold.
// When the input stream ends, we output all results (k, b), where k is
// a key and b is the aggregate for k.
//
// The keys in the output should be given in the order of their first occurrence
// in the input stream. That is, if k1 occurred earlier than k2 in the input
// stream, then the output (k1, b1) should be given before (k2, b2) in the
// output.

public class GroupBy<K,A,B> implements Query<Pair<K,A>,Pair<K,B>> {

	// TODO

	// Initial value
	private final B init;

	// Fold operator
	private final Func2<B, A, B> op;

	// Map of k -> b aggregations for each k
	private final Map<K, B> aggs = new HashMap<>();
	// FIFO order of k values seen as they come through the input channel
	private final List<K> kFIFO = new ArrayList<>();
	

	private GroupBy(B init, Func2<B,A,B> op) {
		// TODO
		this.init = init;
		this.op = op;
	}

	public static <K,A,B> GroupBy<K,A,B> from(B init, Func2<B,A,B> op) {
		return new GroupBy<>(init, op);
	}

	@Override
	public void start(Sink<Pair<K,B>> sink) {
		// TODO
	}

	@Override
	public void next(Pair<K,A> item, Sink<Pair<K,B>> sink) {
		// TODO
		K k = item.getLeft();
		A a = item.getRight();

		// New k seen, add into the FIFO order
		if (!aggs.containsKey(k)) {
			// k is first mapped to the initial value
			aggs.put(k, init);
			kFIFO.add(k);
		}
		
		// Aggregate a into the running b
		B b = aggs.get(k);
		B b_prime = op.apply(b, a);
		aggs.put(k, b_prime);
	}

	@Override
	public void end(Sink<Pair<K,B>> sink) {
		// TODO
		
		// output k->b in FIFO order
		for (K key : kFIFO) {
			B b = aggs.get(key);
			sink.next(Pair.from(key, b));
		}
		sink.end();
	}
	
}
