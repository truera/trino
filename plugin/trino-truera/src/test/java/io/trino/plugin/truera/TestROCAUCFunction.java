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
        boolean[] testLabels1 = new boolean[10];
        testLabels1[9] = true;
        double[] testProbabilities1= new double[]{0.21206135, 0.97905249, 0.6460657 , 0.83698787, 0.40314617,
                0.62190361, 0.34917899, 0.88604834, 0.09936481, 0.65903197};
        assertEquals(String.format("%.2f",ROCAUCFunction.computeRocAuc(testLabels1, testProbabilities1)), "0.67");

        boolean[] testLabels2 = new boolean[10];
        testLabels2[9] = true;
        double[] testProbabilities2 = new double[10];
        Arrays.fill(testProbabilities2, 1.0);
        assertEquals(ROCAUCFunction.computeRocAuc(testLabels2, testProbabilities2), "0.5");
    }

}
