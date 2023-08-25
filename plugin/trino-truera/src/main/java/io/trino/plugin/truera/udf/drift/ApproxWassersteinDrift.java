package io.trino.plugin.truera.udf.drift;

import com.clearspring.analytics.stream.quantile.QDigest;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
            @SqlType("qdigest(double)") Slice leftSketchSlice,
            @SqlType("qdigest(double)") Slice rightSketchSlice,
            @SqlType(StandardTypes.DOUBLE) double accuracy) {
        long startTime = Instant.now().toEpochMilli();
        long prevTime = startTime;
        Logger.getGlobal().info("Starting approx_wasserstein_drift at time: "+ prevTime);
        prevTime = Instant.now().toEpochMilli();

        final QuantileDigest leftSketch = new QuantileDigest(leftSketchSlice);
        final QuantileDigest rightSketch = new QuantileDigest(rightSketchSlice);
        Logger.getGlobal().info("Sketch deserialization took: "+ (Instant.now().toEpochMilli() - prevTime));
        prevTime = Instant.now().toEpochMilli();

        final List<Double> fractions = getBinsFromRange(0., 1., (int) (1/accuracy));
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
        double result = trapezoidalIntegration(quantileDiff, Doubles.toArray(fractions));
        Logger.getGlobal().info("trapezoidalIntegration took:"+ (Instant.now().toEpochMilli() - prevTime));
        Logger.getGlobal().info("approx_wasserstein_drift udf took:"+ (Instant.now().toEpochMilli() - startTime));
        return  result;
    }

    public static double[] getQuantileValues(QuantileDigest quantileDigest, List<Double> fractions) {
        // Need to transform it as done in QuantileDigestFunctions.valueAtQuantileDouble .
        List<Long> rawQuantileLongValues = quantileDigest.getQuantiles(fractions);
        final double[] quantiles = new double[rawQuantileLongValues.size()];
        for(int i=0;i<rawQuantileLongValues.size();i++){
            quantiles[i] = sortableLongToDouble(rawQuantileLongValues.get(i));
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

    public static List<Double> getBinsFromRange(double start, double stop, int numBins) {
        return LongStream.rangeClosed(0, numBins).mapToDouble(i -> start + i * ((stop - start) / (float) numBins)).boxed().toList();
    }
}
