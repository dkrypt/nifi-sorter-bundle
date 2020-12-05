package io.github.dkrypt.processors.sorter;

import java.util.List;

public class QuickSort implements Sorter {
    @Override
    public List<Integer> sort(List<Integer> unsortedList, String order) throws Exception {
        if(!order.equalsIgnoreCase("increasing") && !order.equalsIgnoreCase("decreasing")) {
            throw new Exception(new Exception("Illegal Sorting Order provided"));
        }
        return order.equalsIgnoreCase("increasing") ? increasingSort(unsortedList, 0, unsortedList.size() - 1) : decreasingSort(unsortedList, 0, unsortedList.size() - 1);
    }

    private List<Integer> increasingSort(List<Integer> unsortedList, int low, int high){
        if (low < high) {
            int pi = increasingPartition(unsortedList, low, high);
            increasingSort(unsortedList, low, pi - 1);
            increasingSort(unsortedList, pi + 1, high);
        }
        return unsortedList;
    }
    private List<Integer> decreasingSort(List<Integer> unsortedList, int low, int high){
        if (low < high) {
            int pi = decreasingPartition(unsortedList, low, high);
            decreasingSort(unsortedList, low, pi - 1);
            decreasingSort(unsortedList, pi + 1, high);
        }
        return unsortedList;
    }
    private int increasingPartition(List<Integer> unsortedList, int low, int high) {
        int pivot = unsortedList.get(high);
        int i = low - 1;
        for(int j = low; j < high; j++) {
            if (unsortedList.get(j) < pivot) {
                i++;
                int temp = unsortedList.get(i);
                unsortedList.set(i, unsortedList.get(j));
                unsortedList.set(j, temp);
            }
        }
        int temp = unsortedList.get(i+1);
        unsortedList.set(i+1, unsortedList.get(high));
        unsortedList.set(high, temp);
        return i + 1;
    }
    private int decreasingPartition(List<Integer> unsortedList, int low, int high) {
        int pivot = unsortedList.get(high);
        int i = low - 1;
        for(int j = low; j < high; j++) {
            if (unsortedList.get(j) > pivot) {
                i++;
                int temp = unsortedList.get(i);
                unsortedList.set(i, unsortedList.get(j));
                unsortedList.set(j, temp);
            }
        }
        int temp = unsortedList.get(i+1);
        unsortedList.set(i+1, unsortedList.get(high));
        unsortedList.set(high, temp);
        return i + 1;
    }
}
