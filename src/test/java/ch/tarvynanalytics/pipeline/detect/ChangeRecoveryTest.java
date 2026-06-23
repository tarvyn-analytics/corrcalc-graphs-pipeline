package ch.tarvynanalytics.pipeline.detect;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetector;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.graphs.algos.FireArm;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The architectural reason S3 is on the crypto critical path (crypto-n8-verdict §4): in an
 * already-fused regime the absolute density stays saturated near 1.0, so the density-level baseline
 * (CUSUM on density) has no headroom and cannot fire — but the structure is still <em>moving</em>,
 * so the S3 change detector (CUSUM on weighted change) recovers it. This test drives both detectors
 * over the same saturated-but-changing matrix stream and asserts the baseline misses while S3 fires.
 */
class ChangeRecoveryTest {

    // k=0, h=2 toy tuning (production uses DetectorConfig.crypto()); both detectors share it.
    private static final DetectorConfig CFG = new DetectorConfig(0.0, 2.0, 99.0, 0.5, 1.0, FireArm.UPPER);

    private static double[][] corr3(double rho) {
        return new double[][]{
                {1.0, rho, rho},
                {rho, 1.0, rho},
                {rho, rho, 1.0}};
    }

    @Test
    void saturatedButChangingRegime_BaselineMisses_S3ChangeDetectorRecovers() {
        // Calm: constant rho=0.6 -> density 1.0 (all pairs |0.6|>0.5), zero change.
        double[] calmDensity = {1.0, 1.0, 1.0, 1.0, 1.0};
        double[] calmChange = {0.0, 0.0, 0.0, 0.0, 0.0};
        Calibration baselineCal = ChangeDetectors.calibrate(calmDensity, calmDensity, CFG);   // CUSUM on density
        Calibration changeCal = ChangeDetectors.calibrate(calmChange, calmDensity, CFG);       // CUSUM on change

        // Event: rho alternates 0.95/0.6 -> density stays 1.0 (saturated), but each transition moves
        // |Δr| = 0.35, so the change CUSUM accumulates while the density CUSUM sees a flat 1.0.
        double[] eventRho = {0.95, 0.6, 0.95, 0.6, 0.95, 0.6, 0.95};
        double[] eventDensity = new double[eventRho.length];
        java.util.Arrays.fill(eventDensity, 1.0);

        // Baseline density-level detector: never fires (density is flat at the saturated level).
        Optional<CusumLevelAlert.Fire> baselineFire =
                CusumLevelAlert.firstFire(eventDensity, eventDensity, baselineCal, CFG);
        assertTrue(baselineFire.isEmpty(), "the density-level baseline must miss a saturated regime");

        // S3 change detector over the real matrices: fires once the change CUSUM crosses h.
        ChangeDetector detector = ChangeDetectors.create(3, CFG, changeCal);
        detector.onMatrix(corr3(0.6));   // seed (no transition)
        boolean firedSomewhere = false;
        for (double rho : eventRho) {
            ChangeSignal signal = detector.onMatrix(corr3(rho));
            firedSomewhere |= signal.fired();
        }
        assertTrue(firedSomewhere, "the S3 change detector must recover the saturated-but-changing regime");
        assertTrue(detector.firstFire().isPresent());
        assertFalse(baselineFire.isPresent());
    }
}
