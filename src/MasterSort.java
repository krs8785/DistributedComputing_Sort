
public class MasterSort extends Thread {
	
	private int [] numList;
	private JobManager jobManager;
	private String jobFileName;
	
	public MasterSort(int [] list, JobManager mngr, String jfName) {
		numList = list;
		jobManager = mngr;
		jobFileName = jfName;
	}
	
	public void run() {
		mergeSort(numList);
	}
	
	private void mergeSort(int numList[]) {
		int listCount = numList.length;
		if (listCount == 1)
			return;
		int mid = listCount / 2;
		int leftList[] = new int[mid];
		int rightList[] = new int[listCount - mid];
		for (int i = 0; i < mid; i++) {
			leftList[i] = numList[i];
		}
		for (int i = mid; i < listCount; i++) {
			rightList[i - mid] = numList[i];
		}
		mergeSort(leftList);
		mergeSort(rightList);
		mergeFj(leftList, rightList, numList);
		
		jobManager.masterSortOnSuccess(numList, jobFileName);
	}

	private void mergeFj(int[] leftList, int[] rightList, int sortList[]) {
		int lsize = leftList.length;
		int rsize = rightList.length;
		int lindex = 0;
		int rindex = 0;
		int uindex = 0;

		//Compares the values and puts back the in sorted  order.
		for (lindex = 0, rindex = 0, uindex = 0; lindex < lsize
				&& rindex < rsize;) {
			if (leftList[lindex] >= rightList[rindex])
				sortList[uindex++] = rightList[rindex++];
			else if (leftList[lindex] <= rightList[rindex])
				sortList[uindex++] = leftList[lindex++];
		}
		
		//for excess  values.
		while (lindex < lsize) {
			sortList[uindex++] = leftList[lindex++];
		}
		while (rindex < rsize) {
			sortList[uindex++] = rightList[rindex++];
		}
	}
}
