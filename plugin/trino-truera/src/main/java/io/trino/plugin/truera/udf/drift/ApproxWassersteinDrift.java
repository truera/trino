package io.trino.plugin.truera.udf.drift;

import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static io.trino.operator.scalar.QuantileDigestFunctions.DEFAULT_ACCURACY;
import static io.trino.operator.scalar.QuantileDigestFunctions.valueAtQuantileBigint;

public class ApproxWassersteinDrift {

    public static final double ACCURACY = 0.001;

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
        final double[] fractions = getBinsFromRange(0., 1., (long) (1/accuracy));
        final double[] leftQuantiles = Arrays.stream(fractions).map(
                quantile -> sortableLongToDouble(valueAtQuantileBigint(leftSketch, quantile))
        ).toArray();
        final double[] rightQuantiles = Arrays.stream(fractions).map(
                quantile -> sortableLongToDouble(valueAtQuantileBigint(rightSketch, quantile))
        ).toArray();

        final double wasserstein =
                trapezoidalIntegration(
                        IntStream.range(0, leftQuantiles.length).mapToDouble(i -> Math.abs(rightQuantiles[i] - leftQuantiles[i])).toArray(), fractions);

        return wasserstein;
    }

    public static double trapezoidalIntegration(double[] ys, double[] xs) {
        Preconditions.checkArgument(ys.length == xs.length, "Array lengths don't match!");
        double area = 0.;
        for (int i = 0; i < ys.length - 1; i++) {
            area += (ys[i] + ys[i + 1]) * (xs[i + 1] - xs[i]) / 2;
        }
        return area;
    }

    public static double[] getBinsFromRange(double start, double stop, long numBins) {
        return LongStream.rangeClosed(0, numBins).mapToDouble(i -> start + i * ((stop - start) / (float) numBins)).toArray();
    }
}
