package io.github.dkrypt.processors.sorter;

import java.util.List;

public class MergeSort implements Sorter {
    @Override
    public List<Integer> sort(List<Integer> unsortedList, String order) throws Exception {
        if(!order.equalsIgnoreCase("increasing") && !order.equalsIgnoreCase("decreasing")) {
            throw new Exception(new Exception("Illegal Sorting Order provided"));
        }
        return order.equalsIgnoreCase("increasing") ? increasingSort(unsortedList, 0, unsortedList.size() - 1) : decreasingSort(unsortedList, 0, unsortedList.size() - 1);
    }
    private List<Integer> increasingSort(List<Integer> unsortedList, int l, int r){
        if (l < r) {
            int mid = (l+r)/2;
            increasingSort(unsortedList, l, mid);
            increasingSort(unsortedList, mid + 1, r);
            mergeIncreasing(unsortedList, l, mid, r);
        }
        return unsortedList;
    }
    private List<Integer> decreasingSort(List<Integer> unsortedList, int l, int r){
        if (l < r) {
            int mid = (l+r)/2;
            decreasingSort(unsortedList, l, mid);
            decreasingSort(unsortedList, mid + 1, r);
            mergeDecreasing(unsortedList, l, mid, r);
        }
        return unsortedList;
    }
    void mergeIncreasing(List<Integer> unsortedList, int l, int m, int r) {
        // Find sizes of two sub-arrays to be merged
        int n1 = m - l + 1;
        int n2 = r - m;
        // Create temp arrays
        int L[] = new int[n1];
        int R[] = new int[n2];
        // Copy data into temp arrays
        for(int i = 0; i < n1; i++)
            L[i] = unsortedList.get(l + i);
        for(int j = 0; j < n2; j++)
            R[j] = unsortedList.get(m+1+j);

        /* Merge the temp arrays */

        // Initial indexes of first and second sub-arrays
        int i = 0, j = 0;
        int k = l;
        while(i < n1 && j < n2) {
            if(L[i] <= R[j]) {
                unsortedList.set(k, L[i]);
                i++;
            } else {
                unsortedList.set(k, R[j]);
                j++;
            }
            k++;
        }
        // Copy remaining elements of L[] if any
        while (i < n1) {
            unsortedList.set(k, L[i]);
            i++;
            k++;
        }
        // Copy remaining elements of R[] if any
        while (j < n2) {
            unsortedList.set(k, R[j]);
            j++;
            k++;
        }
    }
    void mergeDecreasing(List<Integer> unsortedList, int l, int m, int r) {
        // Find sizes of two sub-arrays to be merged
        int n1 = m - l + 1;
        int n2 = r - m;
        // Create temp arrays
        int L[] = new int[n1];
        int R[] = new int[n2];
        // Copy data into temp arrays
        for(int i = 0; i < n1; i++)
            L[i] = unsortedList.get(l + i);
        for(int j = 0; j < n2; j++)
            R[j] = unsortedList.get(m+1+j);

        /* Merge the temp arrays */

        // Initial indexes of first and second sub-arrays
        int i = 0, j = 0;
        int k = l;
        while(i < n1 && j < n2) {
            if(L[i] >= R[j]) {
                unsortedList.set(k, L[i]);
                i++;
            } else {
                unsortedList.set(k, R[j]);
                j++;
            }
            k++;
        }
        // Copy remaining elements of L[] if any
        while (i < n1) {
            unsortedList.set(k, L[i]);
            i++;
            k++;
        }
        // Copy remaining elements of R[] if any
        while (j < n2) {
            unsortedList.set(k, R[j]);
            j++;
            k++;
        }
    }
}
