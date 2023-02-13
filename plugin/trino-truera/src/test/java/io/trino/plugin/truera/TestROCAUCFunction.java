package io.trino.plugin.truera;

import java.util.Arrays;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
public class TestROCAUCFunction {
    @Test
    public void testComputeAucRocWhenUndefined() {

        boolean[] testLabels = new boolean[10];
        double[] testProbabilities = new double[10];
        assertEquals(ROCAUCFunction.computeRocAuc(testLabels, testProbabilities), Double.POSITIVE_INFINITY);
    }

    @Test
    public void testComputeAucRoc() {

        boolean[] testLabels = new boolean[10];
        testLabels[9] = true;
        double[] testProbabilities = new double[]{0.21206135, 0.97905249, 0.6460657 , 0.83698787, 0.40314617,
                0.62190361, 0.34917899, 0.88604834, 0.09936481, 0.65903197};
//        Arrays.fill(testProbabilities, 1.0);
//        for (boolean element: testLabels) {
//            System.out.println(element);
//        }
//        for (double element: testProbabilities) {
//            System.out.println(element);
//        }
        System.out.println("score");
        System.out.println(ROCAUCFunction.computeRocAuc(testLabels, testProbabilities));
    }

}
