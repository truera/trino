package io.trino.plugin.truera;

import java.util.Comparator;
import java.util.stream.IntStream;

public class ROCAUCFunction {
    public static double computeRocAuc(boolean[] labels, double[] scores) {
        int[] sortedIndices = IntStream.range(0, scores.length).boxed().sorted(
                Comparator.comparing(i -> scores[i], Comparator.reverseOrder())
        ).mapToInt(i->i).toArray();

//        for (int element: sortedIndices) {
//            System.out.println(element);
//        }

        int currTruePositives = 0, currFalsePositives = 0;
        double auc = 0.;

        for (int i : sortedIndices) {
            int prevTruePositives = currTruePositives;
            int prevFalsePositives = currFalsePositives;
            if (labels[i]) { currTruePositives++; } else { currFalsePositives++; }
//            System.out.println("FP");
//            System.out.println(prevFalsePositives);
//            System.out.println(currFalsePositives);
//            System.out.println("TP");
//            System.out.println(prevTruePositives);
//            System.out.println(currTruePositives);
            auc += trapezoidIntegrate(prevFalsePositives, currFalsePositives, prevTruePositives, currTruePositives);
//            System.out.println("auc");
//            System.out.println(auc);
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
