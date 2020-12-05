package io.github.dkrypt.processors.sorter;

import java.util.List;

public interface Sorter {
    public List<Integer> sort(List<Integer>unsortedList, String order) throws Exception;
}
