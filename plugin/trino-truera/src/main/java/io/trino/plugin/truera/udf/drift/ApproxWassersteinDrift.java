package io.trino.plugin.truera.udf.drift;

import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.Instant;
import java.util.logging.Logger;

import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static io.trino.operator.scalar.QuantileDigestFunctions.valueAtQuantileBigint;

public class ApproxWassersteinDrift {

    public static final double DEFAULT_ACCURACY = 0.001;

    @ScalarFunction(value="approx_wasserstein_drift", deterministic = true)
    @Description("Returns ApproxWassersteinDrift between 2 sketches")
    @SqlType(StandardTypes.DOUBLE)
    public static double approxWassersteinDrift(
            @SqlType("qdigest(double)") Slice leftSketch,
            @SqlType("qdigest(double)") Slice rightSketch) {
        return approxWassersteinDrift(leftSketch,rightSketch, DEFAULT_ACCURACY);
    }

    @ScalarFunction(value="approx_wasserstein_drift", deterministic = true)
    @Description("Returns ApproxWassersteinDrift between 2 sketches")
    @SqlType(StandardTypes.DOUBLE)
    public static double approxWassersteinDrift(
            @SqlType("qdigest(double)") Slice leftSketch,
            @SqlType("qdigest(double)") Slice rightSketch,
            @SqlType(StandardTypes.DOUBLE) double accuracy) {
        long prevTime = Instant.now().toEpochMilli();
        Logger.getGlobal().info("Starting approx_wasserstein_drift at time: "+ prevTime);
        prevTime = Instant.now().toEpochMilli();
        final double[] fractions = getBinsFromRange(0., 1., (int) (1/accuracy));
        Logger.getGlobal().info("getBinsFromRange took:"+ (Instant.now().toEpochMilli() - prevTime));
        prevTime = Instant.now().toEpochMilli();
        final double[] leftQuantiles = getQuantileValues(leftSketch, fractions);
        final double[] rightQuantiles = getQuantileValues(rightSketch, fractions);
        Logger.getGlobal().info("quantile creation took:"+ (Instant.now().toEpochMilli() - prevTime));
        prevTime = Instant.now().toEpochMilli();
        final double[] quantileDiff = new double[leftQuantiles.length];
        for(int i=0;i<leftQuantiles.length; i++){
            quantileDiff[i] = Math.abs(rightQuantiles[i] - leftQuantiles[i]);
        }
        double result = trapezoidalIntegration(quantileDiff, fractions);
        Logger.getGlobal().info("trapezoidalIntegration took:"+ (Instant.now().toEpochMilli() - prevTime));
        return  result;
    }

    public static double[] getQuantileValues(Slice qDigestSketch, double[] fractions) {
        final double[] quantiles = new double[fractions.length];
        for(int i=0;i<fractions.length;i++){
            quantiles[i] = sortableLongToDouble(valueAtQuantileBigint(qDigestSketch, fractions[i]));
        }
        return quantiles;
    }

    public static double trapezoidalIntegration(double[] ys, double[] xs) {
        Preconditions.checkArgument(ys.length == xs.length, "Array lengths don't match!");
        double area = 0.;
        for (int i = 0; i < ys.length - 1; i++) {
            area += (ys[i] + ys[i + 1]) * (xs[i + 1] - xs[i]) / 2;
        }
        return area;
    }

    public static double[] getBinsFromRange(double start, double stop, int numBins) {
        final double[] fractions = new double[numBins];
        for(int i=0;i<numBins;i++){
             fractions[i] = start + i * ((stop - start) / (float) numBins);
        }
        return fractions;
    }
}
