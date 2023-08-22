package io.trino.plugin.truera.udf.state;

import io.trino.plugin.truera.metrics.AUCAccumulatorStateFactory;
import io.trino.plugin.truera.metrics.AUCAccumulatorStateSerializer;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import org.apache.datasketches.kll.KllDoublesSketch;

@AccumulatorStateMetadata(stateFactoryClass = ApproxDriftAccumulatorStateFactory.class, stateSerializerClass = ApproxDriftAccumulatorStateSerializer.class)
public interface ApproxDriftAccumulatorState extends AccumulatorState {

    KllDoublesSketch getLhsSketch();

    KllDoublesSketch getRhsSketch();

    void setLhsSketch(KllDoublesSketch lhsSketch);
    void setRhsSketch(KllDoublesSketch rhsSketch);

    long getEstimatedSize();
}
