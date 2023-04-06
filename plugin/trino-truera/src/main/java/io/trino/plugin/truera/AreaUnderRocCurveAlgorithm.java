package io.trino.plugin.truera;

import java.util.Comparator;
import io.airlift.log.Logger;
import java.util.stream.IntStream;

public class AreaUnderRocCurveAlgorithm {
    private static final Logger log = Logger.get(AreaUnderRocCurveAlgorithm.class);
    public static double computeRocAuc(boolean[] labels, double[] scores) {
        log.info("cow");
        log.info("compute", labels.toString(), scores.toString());
        int[] sortedIndices = IntStream.range(0, scores.length).boxed().sorted(
                Comparator.comparing(i -> scores[i], Comparator.reverseOrder())
        ).mapToInt(i->i).toArray();

        int currTruePositives = 0, currFalsePositives = 0;
        double auc = 0.;

        int i = 0;
        while (i < sortedIndices.length) {
            int prevTruePositives = currTruePositives;
            int prevFalsePositives = currFalsePositives;
            double currentScore = scores[sortedIndices[i]];
            while (i < sortedIndices.length && currentScore == scores[sortedIndices[i]]) {
                if (labels[sortedIndices[i]]) {
                    currTruePositives++;
                } else {
                    currFalsePositives++;
                }
                ++i;
            }
            auc += trapezoidIntegrate(prevFalsePositives, currFalsePositives, prevTruePositives, currTruePositives);
        }

        // If labels only contain one class, AUC is undefined
        if (currTruePositives == 0 || currFalsePositives == 0) {
            return Double.NaN;
        }

        return auc / (currTruePositives * currFalsePositives);
    }

    private static double trapezoidIntegrate(double x1, double x2, double y1, double y2) {
        return (y1 + y2) * Math.abs(x2 - x1) / 2; // (base1 + base2) * height / 2
    }
}
