package ecg;

import dsl.S;
import utils.Pair;
import dsl.Q;
import dsl.Query;

// This file is devoted to the analysis of the heart rate of the patient.
// It is assumed that PeakDetection.qPeaks() has already been implemented.

public class HeartRate {

	// RR interval length (in milliseconds)
	public static Query<Integer, Double> qIntervals() {
		// TODO
		return Q.pipeline(
				PeakDetection.qPeaks(),
				Q.sWindow2((a, b) -> (b - a) * (1000.0 / 360.0)));
	}

	// Average heart rate (over entire signal) in bpm.
	public static Query<Integer, Double> qHeartRateAvg() {
		// TODO
		return Q.pipeline(
				qIntervals(),
				Q.map(rr -> 60000.0 / rr),
				Q.foldAvg());
	}

	// Standard deviation of NN interval length (over the entire signal)
	// in milliseconds.
	public static Query<Integer, Double> qSDNN() {
		// TODO
		return Q.pipeline(
				qIntervals(),
				Q.foldStdev());
	}

	// RMSSD measure (over the entire signal) in milliseconds.
	public static Query<Integer, Double> qRMSSD() {
		// TODO
		return Q.pipeline(
				qIntervals(),
				Q.sWindow2((x, y) -> (y - x) * (y - x)),
				Q.foldAvg(),
				Q.map(Math::sqrt));
	}

	// Proportion (in %) derived by dividing NN50 by the total number
	// of NN intervals (calculated over the entire signal).
	public static Query<Integer, Double> qPNN50() {
		// TODO
		return Q.pipeline(
				qIntervals(),
				Q.sWindow2((x, y) -> Math.abs(y - x)),
				Q.fold(Pair.from(0.0, 0.0), (pair, diff) -> Pair.from(
						pair.getLeft() + (diff >= 50.0 ? 1 : 0), 
						pair.getRight() + 1 
				)),
				Q.map(p -> 100.0 * p.getLeft() / p.getRight()));
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for the Heart Rate *****");
		System.out.println("****************************************");
		System.out.println();

		System.out.println("***** Intervals *****");
		Q.execute(Data.ecgStream("100.csv"), qIntervals(), S.printer());
		System.out.println();

		System.out.println("***** Average heart rate *****");
		Q.execute(Data.ecgStream("100-all.csv"), qHeartRateAvg(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: SDNN *****");
		Q.execute(Data.ecgStream("100-all.csv"), qSDNN(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: RMSSD *****");
		Q.execute(Data.ecgStream("100-all.csv"), qRMSSD(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: pNN50 *****");
		Q.execute(Data.ecgStream("100-all.csv"), qPNN50(), S.printer());
		System.out.println();
	}

}
