package io.trino.plugin.truera.udf.state;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.truera.metrics.AUCAccumulatorState;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;

import java.util.ArrayList;
import java.util.List;

public class ApproxDriftAccumulatorStateSerializer implements AccumulatorStateSerializer<ApproxDriftAccumulatorState> {

    private static final VarbinaryType VARBINARY_TYPE = VarbinaryType.VARBINARY;
    private static final ArrayType VARBINARY_ARRAY_TYPE = new ArrayType(VARBINARY_TYPE);

    @Override
    public Type getSerializedType() {
        return VARBINARY_ARRAY_TYPE;
    }

    @Override
    public void serialize(ApproxDriftAccumulatorState state, BlockBuilder out) {
        final byte[] leftSketchBytes = state.getLhsSketch().toByteArray();
        final byte[] rightSketchBytes = state.getRhsSketch().toByteArray();

        BlockBuilder leftSketchBlockBuilder = VARBINARY_TYPE.createBlockBuilder(null, leftSketchBytes.length);
        BlockBuilder rightSketchBlockBuilder = VARBINARY_TYPE.createBlockBuilder(null, rightSketchBytes.length);
        VARBINARY_TYPE.writeSlice(leftSketchBlockBuilder,Slices.wrappedBuffer(leftSketchBytes));
        VARBINARY_TYPE.writeSlice(rightSketchBlockBuilder,Slices.wrappedBuffer(rightSketchBytes));
        BlockBuilder arrayBlockBuilder = VARBINARY_ARRAY_TYPE.createBlockBuilder(null,2);
        VARBINARY_ARRAY_TYPE.writeObject(arrayBlockBuilder, leftSketchBlockBuilder.build());
        VARBINARY_ARRAY_TYPE.writeObject(arrayBlockBuilder, rightSketchBlockBuilder.build());

    }

    @Override
    public void deserialize(Block block, int index, ApproxDriftAccumulatorState state) {
        if (block.isNull(index)) {
            state.setLhsSketch(null);
            state.setRhsSketch(null);
            return;
        }
        ArrayType arrayType = new ArrayType(VARBINARY_ARRAY_TYPE);
        ArrayBlock arrayBlock = (ArrayBlock) arrayType.getObject(block, index);
        final Slice lhsSlice = VARBINARY_TYPE.getSlice(arrayBlock, 0);
        final Slice rhsSlice = VARBINARY_TYPE.getSlice(arrayBlock, 1);
        final KllDoublesSketch lhsSketch = KllDoublesSketch.heapify(Memory.wrap(lhsSlice.byteArray()));
        final KllDoublesSketch rhsSketch = KllDoublesSketch.heapify(Memory.wrap(rhsSlice.byteArray()));
        state.setLhsSketch(lhsSketch);
        state.setLhsSketch(rhsSketch);

    }
}

