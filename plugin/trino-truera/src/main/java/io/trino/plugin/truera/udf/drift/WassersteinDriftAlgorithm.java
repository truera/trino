package io.trino.plugin.truera.udf.drift;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class WassersteinDriftAlgorithm {

    /**
     * Returns wasserstein distance between two lists using the same approach as scikit learn.
     * <a href="https://github.com/scipy/scipy/blob/v1.11.2/scipy/stats/_stats_py.py#L9896">Scikit Source.</a>
     */
    public static double computeWassersteinScikit(List<Double> leftSeries, List<Double> rightSeries) {
        long lastTime = Instant.now().toEpochMilli();
        List<Double> allSeries = new ArrayList<>(leftSeries);
        allSeries.addAll(rightSeries);
        Collections.sort(allSeries);
        final List<Double> leftSorted = new ArrayList<>(leftSeries);
        final List<Double> rightSorted = new ArrayList<>(rightSeries);
        Collections.sort(leftSorted);
        Collections.sort(rightSorted);
        // TODO: Use non global logger
        Logger.getGlobal().info("Time taken to run sort all lists: "+(Instant.now().toEpochMilli()-lastTime));
        lastTime = Instant.now().toEpochMilli();
        final double[] leftCdf = buildCdf(leftSorted,allSeries);
        final double[] rightCdf = buildCdf(rightSorted,allSeries);
        Logger.getGlobal().info("Time taken to run cdf creation: "+(Instant.now().toEpochMilli()-lastTime));
        lastTime = Instant.now().toEpochMilli();
        final List<Double> deltas = IntStream.range(0, allSeries.size()-1)
                .mapToDouble(i -> allSeries.get(i+1) - allSeries.get(i))
                .boxed().toList();
        double result = 0.0;
        for(int i=0;i<deltas.size();i++){
            result += Math.abs(leftCdf[i] - rightCdf[i]) * deltas.get(i);
        }
        Logger.getGlobal().info("Time taken to complete compute drift: "+ (Instant.now().toEpochMilli()-lastTime));
        return result;
    }

    /**
     * Similar to np.searchsorted([], 'right')
     * for each element in allList find the right most index i of insertion in partialList such that,
     * partialList[j] <= element for all 0<=j<=i
     */
    public static double[] buildCdf(List<Double> partialList, List<Double> allList) {
        int[] indices = new int[allList.size()-1];
        int prevIndex = 0;
        double prevValue = Double.MIN_VALUE;
        for(int i=0;i<allList.size()-1;i++){
            double item = allList.get(i);
            if(prevValue == item){
                indices[i] = prevIndex;
                continue;
            }
            int newIndex = prevIndex;
            while(newIndex<partialList.size() && partialList.get(newIndex)<=item){
                newIndex += 1;
            }
            prevValue = item;
            prevIndex = newIndex;
            indices[i] = newIndex;
        }
        double[] cdf = new double[indices.length];
        for(int i=0;i<indices.length;i++){
            cdf[i] = indices[i]*1.0/partialList.size();
        }
        return cdf;
    }
}
