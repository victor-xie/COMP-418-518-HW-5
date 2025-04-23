package ecg;

import dsl.Q;
import dsl.Query;
import dsl.SCollector;
import dsl.Sink;
import java.util.LinkedList;
import java.util.List;

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

public class Detect implements Query<VTL, Long> {

	// Choose this to be two times the average length
	// over the entire signal.
	private static final double THRESHOLD; // TODO

	// TODO
	// Initialize THRESHOLD to 2 * avg(l[n]) using TrainModel
	static {
		SCollector<Double> sink = new SCollector<>();
		Q.execute(
				Data.ecgStream("100-all.csv"),
				TrainModel.qLengthAvg(),
				sink);
		THRESHOLD = 2 * sink.list.get(0);
	}

	// Stores the 40 samples to find a peak
	private List<VTL> buffer;
	// Don't look for threshold 72 samples after finding a peak
	private int cooldown;
	// Tracks whether we are actively searching for a peak (threshold is crossed)
	private boolean sampling;

	public Detect() {
		// TODO
	}

	@Override
	public void start(Sink<Long> sink) {
		// TODO
		buffer = new LinkedList<>();
		cooldown = 0;
		sampling = false;
	}

	@Override
	public void next(VTL item, Sink<Long> sink) {
		// TODO
		// Still on cooldown after last peak
		if (cooldown > 0) {
			cooldown--;
			return;
		}

		// Not yet looking for peak; check threshold
		if (!sampling) {
			// Begin looking for peak starting with the next sample
			if (item.l > THRESHOLD) {
				buffer.clear();
				sampling = true;
				return;
			} else {
				return;
			}
		}

		// Collect 40 samples
		buffer.add(item);

		if (buffer.size() >= 40) {
			// The sample with the max v value is the peak
			VTL peak = buffer.get(0);
			for (VTL vtl : buffer) {
				if (vtl.v >= peak.v) {
					peak = vtl;
				}
			}

			// Return ts of peak
			sink.next(peak.ts);

			// Go on cooldown, stop collecting samples, and clear samples
			cooldown = 72;
			sampling = false;
			buffer.clear();
		}
	}

	@Override
	public void end(Sink<Long> sink) {
		// TODO
		sink.end();
	}

}
