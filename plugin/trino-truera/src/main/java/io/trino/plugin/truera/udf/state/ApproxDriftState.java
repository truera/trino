package io.trino.plugin.truera.udf.state;

import org.apache.datasketches.kll.KllDoublesSketch;

public class ApproxDriftState implements ApproxDriftAccumulatorState {

    private KllDoublesSketch lhsSketch;
    private KllDoublesSketch rhsSketch;

    public ApproxDriftState() {
        this.lhsSketch = KllDoublesSketch.newHeapInstance();
        this.rhsSketch = KllDoublesSketch.newHeapInstance();
    }

    @Override
    public KllDoublesSketch getLhsSketch() {
        return lhsSketch;
    }

    @Override
    public KllDoublesSketch getRhsSketch() {
        return rhsSketch;
    }

    @Override
    public void setLhsSketch(KllDoublesSketch lhsSketch) {
        this.lhsSketch = lhsSketch;
    }

    @Override
    public void setRhsSketch(KllDoublesSketch rhsSketch) {
        this.rhsSketch = rhsSketch;
    }

    @Override
    public long getEstimatedSize() {
        return lhsSketch.toString().length() + rhsSketch.toString().length();
    }
}
