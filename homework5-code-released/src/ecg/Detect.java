package ecg;

import java.util.LinkedList;
import java.util.List;

import dsl.Q;
import dsl.Query;
import dsl.SCollector;
import dsl.Sink;

// The detection algorithm (decision rule) that we described in class
// (or your own slight variant of it).
//
// (1) Determine the threshold using the class TrainModel.
//
// (2) When l[n] exceeds the threhold, search for peak (max x[n] or raw[n])
//     in the next 40 samples.
//
// (3) No peak should be detected for 72 samples after the last peak.
//
// OUTPUT: The timestamp of each peak.

public class Detect implements Query<VTL,Long> {

	// Choose this to be two times the average length
	// over the entire signal.
	private static final double THRESHOLD; // TODO

	// TODO
	static {
		SCollector<Double> sink = new SCollector<>();
		Q.execute(
			Data.ecgStream("100-all.csv"),
			TrainModel.qLengthAvg(),
			sink
		);
		T