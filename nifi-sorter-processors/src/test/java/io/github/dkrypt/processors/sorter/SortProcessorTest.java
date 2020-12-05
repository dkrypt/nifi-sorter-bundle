/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.dkrypt.processors.sorter;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class SortProcessorTest {
    private static final Path UNSORTED_LIST_1 = Paths.get("src/test/resources/unsortedSeq01.txt");
    private static final Path DEC_SORTED_LIST_1 = Paths.get("src/test/resources/decreasingSortedList01.txt");
    private static final Path INC_SORTED_LIST_1 = Paths.get("src/test/resources/increasingSortedList01.txt");

    @Test
    public void testIncreasingSelectionSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.SEL_SORT.getValue());
        testRunner.setProperty("Order", "increasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(INC_SORTED_LIST_1);
        sortedFlowFile.assertAttributeExists("execution time");
        System.out.println("Selection Inc Sort : "+sortedFlowFile.getAttribute("execution time"));
    }
    @Test
    public void testIncreasingInsertionSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.INS_SORT.getValue());
        testRunner.setProperty("Order", "increasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(INC_SORTED_LIST_1);
        sortedFlowFile.assertAttributeExists("execution time");
        System.out.println("Insertion Inc Sort : "+sortedFlowFile.getAttribute("execution time"));
    }
    @Test
    public void testIncreasingSortBubbleSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.BUBBLE_SORT.getValue());
        testRunner.setProperty("Order", "increasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(INC_SORTED_LIST_1);
        sortedFlowFile.assertAttributeExists("execution time");
        System.out.println("Bubble Inc Sort : "+sortedFlowFile.getAttribute("execution time"));
    }
    @Test
    public void testIncreasingSortQuickSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.QUICK_SORT.getValue());
        testRunner.setProperty("Order", "increasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(INC_SORTED_LIST_1);
        sortedFlowFile.assertAttributeExists("execution time");
        System.out.println("Quick Inc Sort : "+sortedFlowFile.getAttribute("execution time"));
    }
    @Test
    public void testIncreasingSortMergeSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.MERGE_SORT.getValue());
        testRunner.setProperty("Order", "increasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(INC_SORTED_LIST_1);
        sortedFlowFile.assertAttributeExists("execution time");
        System.out.println("Merge Inc Sort : "+sortedFlowFile.getAttribute("execution time"));
    }
    @Test
    public void testDecreasingSelectionSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.SEL_SORT.getValue());
        testRunner.setProperty("Order", "decreasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(DEC_SORTED_LIST_1);
    }
    @Test
    public void testDecreasingInsertionSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.INS_SORT.getValue());
        testRunner.setProperty("Order", "decreasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(DEC_SORTED_LIST_1);
    }
    @Test
    public void testDecreasingBubbleSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.BUBBLE_SORT.getValue());
        testRunner.setProperty("Order", "decreasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(DEC_SORTED_LIST_1);
    }
    @Test
    public void testDecreasingQuickSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.QUICK_SORT.getValue());
        testRunner.setProperty("Order", "decreasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(DEC_SORTED_LIST_1);
    }
    @Test
    public void testDecreasingMergeSort() throws IOException {
        TestRunner testRunner = TestRunners.newTestRunner(SortProcessor.class);
        testRunner.setProperty(SortProcessor.SORT_ALGO.getName(), SortProcessor.MERGE_SORT.getValue());
        testRunner.setProperty("Order", "decreasing");
        testRunner.enqueue(UNSORTED_LIST_1);
        testRunner.run();
        MockFlowFile sortedFlowFile = testRunner.getFlowFilesForRelationship(SortProcessor.REL_SORTED.getName()).get(0);
        sortedFlowFile.assertContentEquals(DEC_SORTED_LIST_1);
    }
}
