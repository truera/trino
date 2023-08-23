package io.trino.plugin.truera.udf.drift;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class WassersteinDriftAlgorithm {

    private static final  Logger LOGGER = Logger.getLogger(WassersteinDriftAlgorithm.class.getName());

    private static final double EPSILON = 0.001;
    private static final double FRACTIONS = 1000;

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

}
