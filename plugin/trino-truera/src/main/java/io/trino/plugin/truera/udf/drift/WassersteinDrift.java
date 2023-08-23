package io.trino.plugin.truera.udf.drift;

import io.trino.plugin.truera.metrics.AUCAccumulatorState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;

import java.util.List;

@AggregationFunction("wasserstein_drift")
@Description("Calculates the wassersteins drift")
public class WassersteinDrift {

    @InputFunction
    public static void input(@AggregationState AUCAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double actual, @SqlType(StandardTypes.DOUBLE) double prediction) {
        state.getActuals().add(actual);
        state.getPredictions().add(prediction);
    }

    @CombineFunction
    public static void combine(@AggregationState AUCAccumulatorState state, @AggregationState AUCAccumulatorState otherState) {
        // Can we sort here?
        state.getActuals().addAll(otherState.getActuals());
        state.getPredictions().addAll(otherState.getPredictions());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState AUCAccumulatorState state, BlockBuilder out) {
        List<Double> actuals = state.getActuals();
        List<Double> predictions = state.getPredictions();

        // Validate inputs
        if (actuals == null || predictions == null || actuals.size() != predictions.size()) {
            throw new IllegalArgumentException("Invalid input");
        }

        double wasserstein = WassersteinDriftAlgorithm.computeWassersteinScikit(actuals, predictions);
        DoubleType.DOUBLE.writeDouble(out, wasserstein);
    }
}
