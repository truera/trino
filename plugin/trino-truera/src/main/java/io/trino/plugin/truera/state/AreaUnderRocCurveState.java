package io.trino.plugin.truera.state;

import io.trino.plugin.truera.aggregation.GroupedRocAucCurve;
import io.trino.spi.block.Block;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateFactoryClass = AreaUnderRocCurveStateFactory.class, stateSerializerClass = AreaUnderRocCurveStateSerializer.class)
public interface AreaUnderRocCurveState extends AccumulatorState
{
    GroupedRocAucCurve get();

    void set(GroupedRocAucCurve value);

    void addMemoryUsage(long memory);

    void deserialize(Block serialized);
}
