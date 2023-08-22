package io.trino.plugin.truera.udf.drift;

import com.google.common.base.Preconditions;
import io.trino.plugin.truera.metrics.AUCAccumulatorState;
import io.trino.plugin.truera.udf.state.ApproxDriftAccumulatorState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.kll.KllDoublesSketch;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.plugin.truera.AreaUnderRocCurveAlgorithm.computeRocAuc;

@AggregationFunction("approx_wasserstein_drift")
@Description("Calculates the Area Under the Curve (AUC)")
public class ApproxWassersteinDrift {

    @InputFunction
    public static void input(@AggregationState ApproxDriftAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double col1, @SqlType(StandardTypes.DOUBLE) double col2) {
        state.getLhsSketch().update(col1);
        state.getLhsSketch().update(col2);
    }

    @CombineFunction
    public static void combine(@AggregationState ApproxDriftAccumulatorState state, @AggregationState ApproxDriftAccumulatorState otherState) {
        state.getLhsSketch().merge(otherState.getLhsSketch());
        state.getRhsSketch().merge(otherState.getRhsSketch());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState ApproxDriftAccumulatorState state, BlockBuilder out) {
        final double[] fractions = getBinsFromRange(0., 1., 1000);
        final KllDoublesSketch leftSketch = state.getLhsSketch();
        final KllDoublesSketch rightSketch = state.getRhsSketch();
        final double[] leftQuantiles = leftSketch.getQuantiles(fractions);
        final double[] rightQuantiles = rightSketch.getQuantiles(fractions);

        final double approxDrift = trapezoidalIntegration(
                IntStream.range(0, leftQuantiles.length).mapToDouble(i -> Math.abs(rightQuantiles[i] - leftQuantiles[i])).toArray(), fractions);

        DoubleType.DOUBLE.writeDouble(out, approxDrift);
    }

    public static double[] getBinsFromRange(double start, double stop, int numBins) {
        return IntStream.rangeClosed(0, numBins).mapToDouble(i -> start + i * ((stop - start) / (float) numBins)).toArray();
    }

    public static double trapezoidalIntegration(double[] ys, double[] xs) {
        Preconditions.checkArgument(ys.length == xs.length, "Array lengths don't match!");
        double area = 0.;
        for (int i = 0; i < ys.length - 1; i++) {
            area += (ys[i] + ys[i + 1]) * (xs[i + 1] - xs[i]) / 2;
        }
        return area;
    }
}
