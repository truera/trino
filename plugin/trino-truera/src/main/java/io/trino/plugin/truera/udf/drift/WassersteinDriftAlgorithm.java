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
     * Returns wasserstein distance between two lists using trapezoidal integration.
     * This approach requires using percentile objects to create CDF.
     */
    public static double computeWasserstein(List<Double> leftSeries, List<Double> rightSeries) {
        double[] leftValues = leftSeries.stream().sorted().mapToDouble(Double::doubleValue).toArray();
        double[] rightValues = rightSeries.stream().sorted().mapToDouble(Double::doubleValue).toArray();

        final Percentile percentile = new Percentile();
        final List<Double> leftQuantileValues = IntStream.range(0,1000).mapToDouble(
                i -> {
                    double queryPercentile = i==0? EPSILON : i*100.0/FRACTIONS;
                    return percentile.evaluate(leftValues, queryPercentile);
                }
        ).boxed().toList();
        final List<Double> rightQuantileValues = IntStream.range(0,1000).mapToDouble(
                i -> {
                    double queryPercentile = i==0? EPSILON : i*100.0/FRACTIONS;
                    return percentile.evaluate(rightValues, queryPercentile);
                }
        ).boxed().toList();
        double result = 0.0;
        for(int i=1;i<FRACTIONS-1;i++){
            final double side1 = Math.abs(leftQuantileValues.get(i) - rightQuantileValues.get(i));
            final double side2 = Math.abs(leftQuantileValues.get(i+1) - rightQuantileValues.get(i+1));
            result += 0.5 * (side1 + side2) * 1.0/FRACTIONS;
            LOGGER.info(side1 + ":" + side2 + ":" + result);
        }
        return result;
    }

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

    public static void main(String[] args) {
        List<Double> left = Arrays.asList(0,1, 2, 4, 7)
                .stream().map(Integer::doubleValue).toList();
        List<Double> right = Arrays.asList(1, 3, 5, 8, 10)
                .stream().map(Integer::doubleValue).toList();
        System.out.println(computeWassersteinScikit(left,right));
    }
}
