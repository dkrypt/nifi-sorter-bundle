package io.github.dkrypt.processors.sorter;

import java.util.List;

public class BubbleSort implements Sorter{
    @Override
    public List<Integer> sort(List<Integer> unsortedList, String order) throws Exception {
        if(!order.equalsIgnoreCase("increasing") && !order.equalsIgnoreCase("decreasing")) {
            throw new Exception(new Exception("Illegal Sorting Order provided"));
        }
        return order.equalsIgnoreCase("increasing") ? increasingSort(unsortedList) : decreasingSort(unsortedList);
    }
    private List<Integer> increasingSort(List<Integer> unsortedList){
        int length = unsortedList.size();
        int i,j,temp;
        boolean swapped;
        for(i = 0; i < length - 1; i++){
            swapped = false;
            for(j = 0; j < length - i -1; j++){
                if(unsortedList.get(j) > unsortedList.get(j+1)){
                    temp = unsortedList.get(j);
                    unsortedList.set(j, unsortedList.get(j+1));
                    unsortedList.set(j+1, temp);
                    swapped = true;
                }
            }
            if (swapped == false) break;
        }
        return unsortedList;
    }
    private List<Integer> decreasingSort(List<Integer> unsortedList) {
        int length = unsortedList.size();
        int i,j,temp;
        boolean swapped;
        for(i = 0; i < length - 1; i++){
            swapped = false;
            for(j = 0; j < length - i -1; j++){
                if(unsortedList.get(j) < unsortedList.get(j+1)){
                    temp = unsortedList.get(j);
                    unsortedList.set(j, unsortedList.get(j+1));
                    unsortedList.set(j+1, temp);
                    swapped = true;
                }
            }
            if (swapped == false) break;
        }
        return unsortedList;
    }
}
