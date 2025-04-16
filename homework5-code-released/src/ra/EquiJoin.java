package ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

// A streaming implementation of the equi-join operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the equality predicate f(a) = g(b) holds.

public class EquiJoin<A,B,T> implements Query<Or<A,B>,Pair<A,B>> {

	// TODO
	private final Function<A, T> f;
	private final Function<B, T> g;


	// Maps to provide O(1) access to all matching a or b values without function queries
	// Stores all values a that produce f(a), i.e. f(a) -> [a_1,a_2,...]
	private final Map<T, List<A>> aCache = new HashMap<>();
	// Stores all values b that produce g(b), i.e. f(b) -> [b_1, b_2,...]
	private final Map<T, List<B>> bCache = new HashMap<>();
	
	private EquiJoin(Function<A,T> f, Function<B,T> g) {
		// TODO
		this.f = f;
		this.g = g;
	}

	public static <A,B,T> EquiJoin<A,B,T> from(Function<A,T> f, Function<B,T> g) {
		return new EquiJoin<>(f, g);
	}

	@Override
	public void start(Sink<Pair<A,B>> sink) {
		// TODO
	}

	@Override
	public void next(Or<A,B> item, Sink<Pair<A,B>> sink) {
		// TODO

		// a Value: Check for all b values that produce f(a) = g(b)
		if (item.isLeft()) {
			A a = item.getLeft();
			// Get f(a)
			T key = f.apply(a);
			// Get all matching B values that produce g(b) (list is empty if there is no match)
			List<B> matchingBs = bCache.getOrDefault(key, List.of());
			// Each pair should be sent out
			for (B b : matchingBs) {
				sink.next(Pair.from(a, b));
			}
			
			// Store the a value in its matching f(a) -> a cache
			List<A> aList = aCache.get(key);
			// New f(a) value
			if (aList == null) {
				aList = new ArrayList<>();
				aCache.put(key, aList);
			}
			aList.add(a);
		} else {
			// Same process but for b
			B b = item.getRight();
			T key = g.apply(b);
			List<A> aList = aCache.getOrDefault(key, List.of());
			for (A a : aList) {
				sink.next(Pair.from(a, b));
			}
			List<B> bList = bCache.get(key);
			if (bList == null) {
				bList = new ArrayList<>();
				bCache.put(key, bList);
			}
			bList.add(b);
		}
	}

	@Override
	public void end(Sink<Pair<A,B>> sink) {
		// TODO
		sink.end();
	}
	
}
