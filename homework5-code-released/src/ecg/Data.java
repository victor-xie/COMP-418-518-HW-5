package ecg;

import java.io.FileNotFoundException;
import java.util.Iterator;

public class Data {

	// TODO: Update the path to the datasets
	private static final String PATH =
		"/Users/victorxie/Desktop/COMP-418-518-HW-5/homework5-code-released/data/";

	private Data() {
		// nothing to do
	}

	public static Iterator<Integer> ecgStream(String file) {
		try {
			return new IteratorECG(PATH + file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}
	
	public static void main(String[] args) {
		System.out.println("*********************************");
		System.out.println("********** ECG Dataset **********");
		System.out.println("*********************************");
		System.out.println();

		Iterator<Integer> it = Data.ecgStream("100-samples-100.csv");
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}

}
