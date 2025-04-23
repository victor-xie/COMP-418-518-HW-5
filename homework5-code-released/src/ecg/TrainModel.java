package ecg;

import dsl.Q;
import dsl.Query;
import dsl.S;
import utils.Pair;

public class TrainModel {

	// The average value of the signal l[n] over the entire input.
	public static Query<Integer, Double> qLengthAvg() {
		// TODO
		// Hint: Use PeakDetection.qLength()
		return Q.pipeline(
				// Curve Length l[n]
				PeakDetection.qLength(),

				// Get the sum of the signal and the count of entries
				Q.fold(Pair.from(0.0, 0),
						(acc, x) -> Pair.from(acc.getLeft() + x, acc.getRight() + 1)),

				// Calculate average
				Q.map(p -> (p.getLeft() / p.getRight())));
	}

	public static void main(String[] args) {
		System.out.println("***********************************************");
		System.out.println("***** Algorithm for finding the threshold *****");
		System.out.println("***********************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100-all.csv"), qLengthAvg(), S.printer());
	}

}
