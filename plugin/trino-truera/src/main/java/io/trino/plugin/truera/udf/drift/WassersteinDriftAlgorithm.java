package io.trino.plugin.truera.udf.drift;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WassersteinDriftAlgorithm {

    private static final  Logger LOGGER = Logger.getLogger(WassersteinDriftAlgorithm.class.getName());

    private static final double EPSILON = 0.001;
    private static final double FRACTIONS = 1000;

    /**
     * Returns wasserstein distance between two lists using the same approach as scikit learn.
     * <a href="https://github.com/scipy/scipy/blob/v1.11.2/scipy/stats/_stats_py.py#L9896">Scikit Source.</a>
     */
    public static double computeWassersteinScikit(List<Double> leftSeries, List<Double> rightSeries) {
        List<Double> allSeries = new ArrayList<>(leftSeries);
        allSeries.addAll(rightSeries);
        Collections.sort(allSeries);
        final List<Double> leftSorted = leftSeries.stream().sorted().toList();
        final List<Double> rightSorted = rightSeries.stream().sorted().toList();

        final List<Double> leftCdf = allSeries.subList(0, allSeries.size()-1)
                .stream().map( v -> searchInsertIndexFromRight(leftSorted,v)*1.0/leftSorted.size()).toList();
        final List<Double> rightCdf = allSeries.subList(0, allSeries.size()-1)
                .stream().map( v -> searchInsertIndexFromRight(rightSorted,v)*1.0/rightSorted.size()).toList();
        final List<Double> deltas = IntStream.range(0, allSeries.size()-1)
                .mapToDouble(i -> allSeries.get(i+1) - allSeries.get(i))
                .boxed().toList();
        double result = 0.0;
        for(int i=0;i<deltas.size();i++){
            result += Math.abs(leftCdf.get(i) - rightCdf.get(i)) * deltas.get(i);
        }
        return result;
    }

    public static int searchInsertIndexFromRight(List<Double> list, double value) {
        int insertIndex = Collections.binarySearch(list,value);
        int maxRightIndex = insertIndex;
        if(insertIndex < 0) {
            maxRightIndex = -insertIndex - 1;
        }
        for(int i=maxRightIndex; i<list.size();i++) {
            if (list.get(i).equals(value)){
                maxRightIndex+=1;
            }
        }
        return maxRightIndex;
    }
}
