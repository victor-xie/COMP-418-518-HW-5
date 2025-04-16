package ra;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BiPredicate;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

// A streaming implementation of the theta join operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the pair (a, b) satisfies a predicate theta.

public class ThetaJoin<A,B> implements Query<Or<A,B>,Pair<A,B>> {

	// TODO
	private final BiPredicate<A, B> theta;
    private final Deque<A> leftVals = new ArrayDeque<>();
    private final Deque<B> rightVals = new ArrayDeque<>();

	private ThetaJoin(BiPredicate<A,B> theta) {
		// TODO
		this.theta = theta;
	}

	public static <A,B> ThetaJoin<A,B> from(BiPredicate<A,B> theta) {
		return new ThetaJoin<>(theta);
	}

	@Override
	public void start(Sink<Pair<A,B>> sink) {
		// TODO
	}

	@Override
	public void next(Or<A,B> item, Sink<Pair<A,B>> sink) {
		// TODO

		// Check (a,b) for all currently seen b values
		if (item.isLeft()) {
            A a = item.getLeft();
            for (B b : rightVals) {
				// Apply theta
                if (theta.test(a, b)) {
					// Send the pair if the test passes
                    sink.next(Pair.from(a, b));
                }
            }
			// Store a
            leftVals.addLast(a);
        } else {
			// Same process but check (a,b) for all currently seen a values
            B b = item.getRight();
            for (A a : leftVals) {
                if (theta.test(a, b)) {
                    sink.next(Pair.from(a, b));
                }
            }
			// Store b
            rightVals.addLast(b);
        }
	}

	@Override
	public void end(Sink<Pair<A,B>> sink) {
		// TODO
		sink.end();
	}
	
}
