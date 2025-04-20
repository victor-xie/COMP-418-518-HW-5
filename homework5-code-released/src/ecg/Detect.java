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
		THRESHOLD = sink.list.get(0);  
	}

	// Internal state
	private List<VTL> buffer;
	private int cooldown;
	private boolean collecting;

	public Detect() {
		// TODO
	}

	@Override
	public void start(Sink<Long> sink) {
		// TODO
		buffer = new LinkedList<>();
		cooldown = 0;
		collecting = false;
	}

	@Override
	public void next(VTL item, Sink<Long> sink) {
		// TODO
		// Step 1: Suppress during cooldown
		if (cooldown > 0) {
			cooldown--;
			return;
		}

		// Step 2: Start collecting if triggered by threshold
		if (!collecting) {
			if (item.l > THRESHOLD) {
				buffer.clear();
				collecting = true;
				return;  
			} else {
				return;
			}
		}

		// Step 3: Collect 40 samples
		buffer.add(item);

		if (buffer.size() >= 40) {
			// Step 4: Find max v in the buffer
			VTL peak = buffer.get(0);
			for (VTL vtl : buffer) {
				if (vtl.v >= peak.v) {
					peak = vtl;
				}
			}

			// Step 5: Emit timestamp of max
			sink.next(peak.ts);

			// Step 6: Reset
			cooldown = 72;
			collecting = false;
			buffer.clear();
		}
	}

	@Override
	public void end(Sink<Long> sink) {
		// TODO
		sink.end();
	}
	
}
