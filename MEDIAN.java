/****
* This file implements the Apache Pig MEDIAN UDF for finding the median of a Bag, which is returned as a double.
* The UDF returns the true median for Bags of size < 100 
* The median of larger bags is estimated using the method described here: http://www.cs.ucsb.edu/~suri/cs290/MunroPat.pdf
* The size of the list used to estimate the median (S) is set at 1000, but can be changed depending on requirements.
***/

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.Random;
import java.util.Iterator;
import org.apache.pig.EvalFunc;      
import org.apache.pig.data.Tuple;    
import org.apache.pig.data.DataBag;  
import org.apache.hadoop.io.WritableComparable;  

public class MEDIAN extends EvalFunc<Double> {
		
	private double findMedian(List<Double> list) {
		// returns the true median of list
		int len = list.size();
		
		if (len % 2 == 0) 
			return (list.get(len/2 - 1) + list.get(len/2))/2;
		else 
			return list.get(len/2);
	}
	
	private void insertSorted(List<Double> list, double i) {
		// inserts i into a sorted list
		
		if (i > list.get(list.size()-1)){ // if i bigger than all elements in list, append i to the end
			list.add(i);
		}
		else {  // otherwise, find where it belongs in the list
		int k = 0;
		while (list.get(k) < i){
			k++;
		}
		list.add(k,i);
		}
	}
	
	public Double exec(Tuple input) throws IOException {
	// main method
		int S = 1000;  // S is initialized to 1000. Larger S will lead to higher accuracy, but performance may suffer.
		double L = 0.0, H = 0.0;
		List<Double> S_list = new ArrayList<Double>();
		
		DataBag bag = (DataBag)input.get(0);
		Iterator it = bag.iterator();
		
		int ctr = 0;

		while(it.hasNext() && ctr < S){
			Tuple t = (Tuple)it.next();
			double i = 0.0;
			if (t != null && t.size() > 0 && t.get(0) != null){
				i = (double)t.get(0);
				S_list.add(i);
				ctr++;
				}
		}
		
		if (ctr < S){
			return findMedian(S_list);
		}
		
		Collections.sort(S_list);

		while (it.hasNext()) {
			Tuple t = (Tuple)it.next();
			double i = 0;
			
			if (t != null && t.size() > 0 && t.get(0) != null){
				i = (double)t.get(0);
				}

			if (i > S_list.get(S-1)) {
				H++;
			}
			if (i < S_list.get(0)) {
				L++;
			}
			
			Random randNum = new Random();
			
			if(randNum.nextDouble() < (S-2)/(H+S+L)){
				if (H < L){
					S_list.remove(S-1);
				}
				else {
					S_list.remove(0);
				}
				insertSorted(S_list, i);
			}
		}
		
		return findMedian(S_list);
		}	
	
}
