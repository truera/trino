package io.trino.plugin.truera.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import io.trino.array.BooleanBigArray;
import io.trino.array.IntBigArray;
import io.trino.array.LongBigArray;
import io.trino.array.DoubleBigArray;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class GroupedRocAucCurve {

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedRocAucCurve.class).instanceSize();
    private static final int NULL = -1;

    // one entry per group
    // each entry is the index of the first elements of the group in the labels/scores/nextLinks arrays
    private final LongBigArray headIndices;

    // one entry per double/boolean pair
    private final BooleanBigArray labels;
    private final DoubleBigArray scores;

    // the value in nextLinks contains the index of the next value in the chain
    // a value NULL (-1) indicates it is the last value for the group
    private final IntBigArray nextLinks;

    // the index of the next free element in the labels/scores/nextLinks arrays
    // this is needed to be able to know where to continue adding elements when after the arrays are resized
    private int nextFreeIndex;

    private long currentGroupId = -1;

    public GroupedRocAucCurve() {
        this.headIndices = new LongBigArray(NULL);
        this.labels = new BooleanBigArray();
        this.scores = new DoubleBigArray();
        this.nextLinks = new IntBigArray(NULL);
        this.nextFreeIndex = 0;
    }

    public GroupedRocAucCurve(long groupId, Block serialized) {
        this();
        this.currentGroupId = groupId;

        requireNonNull(serialized, "serialized block is null");
        for (int i = 0; i < serialized.getPositionCount(); i++) {
            Block entryBlock = serialized.getObject(i, Block.class);
            add(entryBlock, entryBlock, 0, 1);
        }
    }

    public void serialize(BlockBuilder out) {
        if (isCurrentGroupEmpty()) {
            out.appendNull();
            return;
        }

        // retrieve scores + labels
        List<Boolean> labelList = new ArrayList<>();
        List<Double> scoreList = new ArrayList<>();

        int currentIndex = (int) headIndices.get(currentGroupId);
        while (currentIndex != NULL) {
            labelList.add(labels.get(currentIndex));
            scoreList.add(scores.get(currentIndex));
            currentIndex = nextLinks.get(currentIndex);
        }

        // convert lists to primitive arrays
        boolean[] labels = new boolean[labelList.size()];
        for (int i = 0; i < labels.length; i++) {
            labels[i] = labelList.get(i);
        }
        double[] scores = scoreList.stream().mapToDouble(Double::doubleValue).toArray();

        // compute + return
        double auc = computeRocAuc(labels, scores);
        if (Double.isFinite(auc)) {
            DoubleType.DOUBLE.writeDouble(out, auc);
        } else {
            out.appendNull();
        }
    }

    public long estimatedInMemorySize() {
        return INSTANCE_SIZE + labels.sizeOf() + scores.sizeOf() + nextLinks.sizeOf() + headIndices.sizeOf();
    }

    public GroupedRocAucCurve setGroupId(long groupId) {
        this.currentGroupId = groupId;
        return this;
    }

    public void add(Block labelsBlock, Block scoresBlock, int labelPosition, int scorePosition) {
        ensureCapacity(currentGroupId + 1);

        labels.set(nextFreeIndex, BooleanType.BOOLEAN.getBoolean(labelsBlock, labelPosition));
        scores.set(nextFreeIndex, DoubleType.DOUBLE.getDouble(scoresBlock, scorePosition));
        nextLinks.set(nextFreeIndex, (int) headIndices.get(currentGroupId));
        nextFreeIndex++;
    }

    public void ensureCapacity(long numberOfGroups) {
        headIndices.ensureCapacity(numberOfGroups);
        int numberOfValues = nextFreeIndex + 1;
        labels.ensureCapacity(numberOfValues);
        scores.ensureCapacity(numberOfValues);
        nextLinks.ensureCapacity(numberOfValues);
    }

    public void addAll(GroupedRocAucCurve other) {
        other.readAll(this);
    }

    public void readAll(GroupedRocAucCurve to) {
        int currentIndex = (int) headIndices.get(currentGroupId);
        while (currentIndex != NULL) {
            BlockBuilder labelBlockBuilder = BooleanType.BOOLEAN.createBlockBuilder(null, 0);
            BooleanType.BOOLEAN.writeBoolean(labelBlockBuilder, labels.get(currentIndex));
            BlockBuilder scoreBlockBuilder = DoubleType.DOUBLE.createBlockBuilder(null, 0);
            DoubleType.DOUBLE.writeDouble(scoreBlockBuilder, scores.get(currentIndex));

            to.add(labelBlockBuilder.build(), scoreBlockBuilder.build(), 0, 0);
            currentIndex = nextLinks.get(currentIndex);
        }
    }

    public boolean isCurrentGroupEmpty() {
        return headIndices.get(currentGroupId) == NULL;
    }

    private static double computeRocAuc(boolean[] labels, double[] scores) {
        int[] sortedIndices = IntStream.range(0, scores.length).boxed().sorted(
                Comparator.comparing(i -> scores[i])
        ).sorted(Collections.reverseOrder()).mapToInt(i->i).toArray();

        int currTruePositives = 0, currFalsePositives = 0, prevTruePositives =0, prevFalsePositives = 0;
        double auc = 0.;

        for (int i : sortedIndices) {
            if (labels[i]) { currTruePositives++; } else { currFalsePositives++; };
            prevTruePositives = currTruePositives;
            prevFalsePositives = currFalsePositives;
            auc += trapezoidIntegrate(prevFalsePositives, currFalsePositives, prevTruePositives, currTruePositives);
        }

        // If labels only contain one class, AUC is undefined
        if (currTruePositives == 0 || currFalsePositives == 0) {
            return Double.POSITIVE_INFINITY;
        }

        return auc / (currTruePositives * currFalsePositives);
    }

    private static double trapezoidIntegrate(double x1, double x2, double y1, double y2) {
        return (y1 + y2) * Math.abs(x2 - x1) / 2; // (base1 + base2) * height / 2
    }

}
