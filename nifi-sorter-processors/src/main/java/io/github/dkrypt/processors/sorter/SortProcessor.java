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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.StringWriter;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"sort", "sorting", "algorithm"})
@CapabilityDescription("This processor sorts a given list of integers according to the provided sorting algorithm and order of sorting")
@WritesAttributes({@WritesAttribute(attribute="execution time", description="Time elapsed for sorting the given series")})
public class SortProcessor extends AbstractProcessor {
    private static final String selection_sort = "Selection Sort";
    private static final String bubble_sort = "Bubble Sort";
    private static final String insertion_sort = "Insertion Sort";
    private static final String quick_sort = "Quick Sort";
    private static final String merge_sort = "Merge Sort";

    public static final AllowableValue SEL_SORT = new AllowableValue(selection_sort, selection_sort, selection_sort);
    public static final AllowableValue BUBBLE_SORT = new AllowableValue(bubble_sort, bubble_sort, bubble_sort);
    public static final AllowableValue INS_SORT = new AllowableValue(insertion_sort, insertion_sort, insertion_sort);
    public static final AllowableValue QUICK_SORT = new AllowableValue(quick_sort,quick_sort,quick_sort);
    public static final AllowableValue MERGE_SORT = new AllowableValue(merge_sort, merge_sort, merge_sort);

    public static final PropertyDescriptor SORT_ALGO = new PropertyDescriptor
            .Builder().name("SORT_ALGO")
            .displayName("Sorting Algorithm")
            .description("Algorithm to be used for sorting")
            .allowableValues(SEL_SORT, BUBBLE_SORT, INS_SORT, QUICK_SORT, MERGE_SORT)
            .required(true)
            .defaultValue(SEL_SORT.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SORTED = new Relationship.Builder()
            .name("Sorted")
            .description("Relationship to route Sorted flowfile to.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Relationship to route failed flowfiles to.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SORT_ALGO);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SORTED);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .description("order of the sorting")
                .dynamic(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .build();
    }

    private String sortingAlgorithm = null;
    private String sortingOrder =  null;
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        sortingAlgorithm = context.getProperty(SORT_ALGO).getValue();
        context.getProperties().forEach((propertyDescriptor, s) -> {
            if(propertyDescriptor.isDynamic())
                sortingOrder = s;
        });
    }
    private Duration executionTime = null;
    private List<Integer> sort(List<Integer> unsorted) throws Exception {
        List<Integer> sortedList = null;
        Instant start;
        switch (sortingAlgorithm) {
            case selection_sort:
                start = Instant.now();
                sortedList = new SelectionSort().sort(unsorted, sortingOrder);
                executionTime = Duration.between(start, Instant.now());
                return sortedList;
            case bubble_sort:
                start = Instant.now();
                sortedList = new BubbleSort().sort(unsorted, sortingOrder);
                executionTime = Duration.between(start, Instant.now());
                return sortedList;
            case insertion_sort:
                start = Instant.now();
                sortedList = new InsertionSort().sort(unsorted, sortingOrder);
                Instant end = Instant.now();
                executionTime = Duration.between(start, Instant.now());
                return sortedList;
            case quick_sort:
                start = Instant.now();
                sortedList = new QuickSort().sort(unsorted, sortingOrder);
                executionTime = Duration.between(start, Instant.now());
                return sortedList;
            case merge_sort:
                start = Instant.now();
                sortedList = new MergeSort().sort(unsorted, sortingOrder);
                executionTime = Duration.between(start, Instant.now());
                return sortedList;
            default:
                throw new ProcessException("Invalid sorted order.");
        }
    }
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get the incoming flowfile
        FlowFile flowFile = session.get();
        if ( flowFile == null ) return;
        boolean errorOccurred = false;
        // Read the contents of flowfile as String
        AtomicReference<String> inputString = new AtomicReference<>();
        session.read(flowFile, (in) -> {
            StringWriter writer = new StringWriter();
            IOUtils.copy(in, writer, Charset.defaultCharset());
            String input = writer.toString();
            inputString.set(input);
        });
        // Separate Strings by "\n" and Parse to Integer
        String[] inputStringList = inputString.get().split("\n");
        List<Integer> inputList = new ArrayList<>();
        for(String number : inputStringList)
            inputList.add(Integer.parseInt(number));
        // Sorting Logic
        AtomicReference<List<Integer>> finalSortedList = new AtomicReference<>();
        try {
            finalSortedList.set(sort(inputList));
        } catch (Exception e) {
            errorOccurred = true;
        }
        flowFile = session.write(flowFile, outputStream -> {
            String out = "";
            for (int sorted : finalSortedList.get()) {
                out = out.concat(Integer.toString(sorted)+"\n");
            }
            outputStream.write(out.getBytes());
        });
        if (errorOccurred) {
        session.transfer(flowFile, REL_FAILURE);
        }
        flowFile = session.putAttribute(flowFile, "execution time", Long.toString(executionTime.multipliedBy(1000).toMillis()));
        session.transfer(flowFile, REL_SORTED);
    }
}
