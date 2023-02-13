package io.trino.plugin.truera;

import java.util.Collections;
import java.util.Comparator;
import java.util.stream.IntStream;

public class ROCAUCFunction {
    public static double computeRocAuc(boolean[] labels, double[] scores) {
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
