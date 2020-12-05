package io.github.dkrypt.processors.sorter;

import java.util.List;

public class InsertionSort implements Sorter{
    @Override
    public List<Integer> sort(List<Integer> unsortedList, String order) throws Exception {
        if(!order.equalsIgnoreCase("increasing") && !order.equalsIgnoreCase("decreasing")) {
            throw new Exception(new Exception("Illegal Sorting Order provided"));
        }
        return order.equalsIgnoreCase("increasing") ? increasingSort(unsortedList) : decreasingSort(unsortedList);
    }
    private List<Integer> increasingSort(List<Integer> unsortedList){
        int length = unsortedList.size();
        for(int i = 1; i < length; i++){
            int key = unsortedList.get(i);
            int j = i -1;
            while(j>=0 && unsortedList.get(j) > key) {
                unsortedList.set(j+1, unsortedList.get(j));
                j = j - 1;
            }
            unsortedList.set(j+1, key);
        }
        return unsortedList;
    }
    private List<Integer> decreasingSort(List<Integer> unsortedList){
        int length = unsortedList.size();
        for(int i = 0; i < length - 1; i++){
            int maxIndex = i;
            for(int j = i+1; j < length; j++)
                if(unsortedList.get(j) > unsortedList.get(maxIndex))
                    maxIndex = j;
            int temp = unsortedList.get(maxIndex);
            unsortedList.set(maxIndex, unsortedList.get(i));
            unsortedList.set(i, temp);
        }
        return unsortedList;
    }
}
