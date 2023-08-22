package io.trino.plugin.truera.udf.state;

import io.trino.plugin.truera.metrics.AUCAccumulatorState;
import io.trino.plugin.truera.metrics.AUCState;
import io.trino.spi.function.AccumulatorStateFactory;

public class ApproxDriftAccumulatorStateFactory implements AccumulatorStateFactory<ApproxDriftAccumulatorState> {
    @Override
    public ApproxDriftAccumulatorState createSingleState() {
        return new ApproxDriftState();
    }

    @Override
    public ApproxDriftAccumulatorState createGroupedState() {
        return new ApproxDriftState();
    }
}
