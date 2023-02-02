package io.trino.plugin.truera.state;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.RowType.anonymousRow;

public class AreaUnderRocCurveStateSerializer implements AccumulatorStateSerializer<AreaUnderRocCurveState>
{
    private static final ArrayType SERIALIZED_TYPE = new ArrayType(anonymousRow(BooleanType.BOOLEAN, DoubleType.DOUBLE));

    @Override
    public Type getSerializedType() {
        return SERIALIZED_TYPE;
    }

    @Override
    public void serialize(AreaUnderRocCurveState state, BlockBuilder out) {
        state.get().serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, AreaUnderRocCurveState state) {
        state.deserialize(SERIALIZED_TYPE.getObject(block, index));
    }

}
