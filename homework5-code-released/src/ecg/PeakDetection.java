package ecg;

import dsl.Q;
import dsl.Query;
import dsl.S;
import utils.Pair;

public class PeakDetection {

	// The curve length transformation:
	//
	// adjust: x[n] = raw[n] - 1024
	// smooth: y[n] = (x[n-2] + x[n-1] + x[n] + x[n+1] + x[n+2]) / 5
	// deriv: d[n] = (y[n+1] - y[n-1]) / 2
	// length: l[n] = t(d[n-w]) + ... + t(d[n+w]), where
	//         w = 20 (samples) and t(d) = sqrt(1.0 + d * d)

	public static Query<Integer,Double> qLength() {
		// adjust >> smooth >> deriv >> length

		// TODO
		return Q.pipeline(
		// Step 1: Adjust — x[n] = raw[n] - 1024
		Q.map(v -> (double)(v - 1024)),

		// Step 2: Smooth — y[n] = avg over 5 samples (centered)
		Q.sWindowNaive(5, 0.0, (sum, x) -> sum + x),
		Q.map(sum -> sum / 5.0),

		// Step 3: Deriv — central difference
		Q.sWindow3((a, b, c) -> (c - a) / 2.0),

		// Step 4: Length — curve length over 2w+1 = 41
		Q.sWindowNaive(41, 0.0, (s, d) -> s + Math.sqrt(1 + d * d))
	);
	}

	// In order to detect peaks we need both the raw (or adjusted)
	// signal and the signal given by the curve length transformation.
	// Use the datatype VTL and implement the class Detect.

	public static Query<Integer,Long> qPeaks() {
		// TODO
		return Q.pipeline(
		// Step 1: Compute curve length l[n] from raw ECG
		Q.parallel(
			qLength(), // Left: l[n] from raw Integer
			Q.pipeline(
				Q.scan(Pair.from(0L, 0), (pair, v) -> Pair.from(pair.getLeft() + 1, v)),
				Q.map(p -> new VT(p.getRight(), p.getLeft() - 1)) // Right: VT(v, ts)
			),
			// Merge l and VT into VTL
			(l, vt) -> vt.extendl(l)
		),

		// Step 2: Run decision rule
		new Detect()
	);
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for Peak Detection *****");
		System.out.println("****************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100.csv"), qPeaks(), S.printer());
	}

}
