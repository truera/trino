package io.trino.plugin.truera.state;

import io.trino.plugin.truera.aggregation.GroupedRocAucCurve;
import io.trino.spi.block.Block;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class AreaUnderRocCurveStateFactory implements AccumulatorStateFactory<AreaUnderRocCurveState>
{
    @Override
    public AreaUnderRocCurveState createSingleState() {
        return new SingleState();
    }

    @Override
    public AreaUnderRocCurveState createGroupedState() {
        return new GroupedState();
    }

    public static class GroupedState  implements AreaUnderRocCurveState, GroupedAccumulatorState {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private GroupedRocAucCurve auc;
        private long size;

        public GroupedState() {
            auc = new GroupedRocAucCurve();
        }

        @Override
        public void setGroupId(long groupId)
        {
            auc.setGroupId(groupId);
        }

        @Override
        public void ensureCapacity(long size) {
            auc.ensureCapacity(size);
        }

        @Override
        public GroupedRocAucCurve get() {
            return auc;
        }

        @Override
        public void set(GroupedRocAucCurve value)
        {
            requireNonNull(value, "value is null");

            GroupedRocAucCurve previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            auc = value;
            size += value.estimatedInMemorySize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public void deserialize(Block serialized)
        {
            this.auc = new GroupedRocAucCurve(0, serialized);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + auc.estimatedInMemorySize();
        }
    }

    public static class SingleState
            implements AreaUnderRocCurveState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private GroupedRocAucCurve auc;

        public SingleState()
        {
            auc = new GroupedRocAucCurve();

            // set synthetic, unique group id to use GroupAreaUnderRocCurve from the single state
            auc.setGroupId(0);
        }

        @Override
        public GroupedRocAucCurve get()
        {
            return auc;
        }

        @Override
        public void deserialize(Block serialized)
        {
            this.auc = new GroupedRocAucCurve(0, serialized);
        }

        @Override
        public void set(GroupedRocAucCurve value)
        {
            auc = value;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (auc != null) {
                estimatedSize += auc.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }


}
