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
	// w = 20 (samples) and t(d) = sqrt(1.0 + d * d)

	public static Query<Integer, Double> qLength() {
		// adjust >> smooth >> deriv >> length

		// TODO
		return Q.pipeline(
				// Adjust
				Q.map(v -> (double) (v - 1024)),

				// Smooth (2 offset due to left side of window)
				Q.sWindowNaive(5, 0.0, (sum, x) -> sum + x),
				Q.map(sum -> sum / 5.0),

				// Deriv (1 offset due to left side of window)
				Q.sWindow3((prev, curr, next) -> (next - prev) / 2.0),

				// Length (20 offset due to left side of window)
				Q.sWindowNaive(41, 0.0, (s, d) -> s + Math.sqrt(1 + d * d)));
	}

	// In order to detect peaks we need both the raw (or adjusted)
	// signal and the signal given by the curve length transformation.
	// Use the datatype VTL and implement the class Detect.

	public static Query<Integer, Long> qPeaks() {
		// TODO
		// Generate VTL input to Detect()
		// Each VTL will contain a l[n], corresponding x[n] (with 23 ts offset)
		// due to the shift from the qLength() transformation (i.e. l[0] corresponds to x[23])
		// with the ts of the x[n] curve (again starting from 23)
		return Q.pipeline(
				// Zip each l[n] with its corresponding x[n+23]
				// i.e. l[0] corresponds to x[23], l[1] to x[24], etc.
				// because the first l value needs 23 values to generate
				Q.parallel(
						qLength(), // l[n] curve
 						Q.pipeline(
								Q.ignore(23), // The first 23 x values must be ignored because they have no corresponding l
								// Give each raw x[n] value (here "v") a timestamp
								// ts starts from 23 because the first 23 x vals are needed to generate l[0]
								Q.scan(Pair.from(23, 0), (pair, v) -> Pair.from(pair.getLeft() + 1, v)),
								// Map to a VT object which holds each raw value with its ts
								// Make the timestamps 0-indexed
								Q.map(p -> new VT(p.getRight(), p.getLeft() - 1)) 
						),
						// Zip the l value into vt and make a VTL object
						(l, vt) -> vt.extendl(l)),

				// Pass the stream of VTL objects into the decision rule and get the peaks
				new Detect());
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for Peak Detection *****");
		System.out.println("****************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100.csv"), qPeaks(), S.printer());
	}

}
