package ecg;

import dsl.Q;
import dsl.Query;
import dsl.S;
import utils.Pair;

public class TrainModel {

	// The average value of the signal l[n] over the entire input.
	public static Query<Integer,Double> qLengthAvg() {
		// TODO
		// Hint: Use PeakDetection.qLength()
		return Q.pipeline(
		// Step 1: Compute curve length stream
		PeakDetection.qLength(),

		// Step 2: Fold to compute (sum, count)
		Q.fold(Pair.from(0.0, 0), (acc, x) ->
			Pair.from(acc.getLeft() + x, acc.getRight() + 1)
		),

		// Step 3: Map to (sum / count)
		Q.map(p -> (p.getLeft() / p.getRight()))
	);
	}

	public static void main(String[] args) {
		System.out.println("***********************************************");
		System.out.println("***** Algorithm for finding the threshold *****");
		System.out.println("***********************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100-all.csv"), qLengthAvg(), S.printer());
	}

}
