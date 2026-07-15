package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.graphs.algos.FireArm;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CusumLevelAlertTest {

    // k=0, h=2: with mu=0, sigma=1, the upper CUSUM is the running sum of the fire values, so a
    // constant series of 1.0 gives S+ = 1,2,3,... and first crosses h=2 at index 2.
    private static final DetectorConfig CFG = new DetectorConfig(0.0, 2.0, 50.0, 0.5, 1.0, FireArm.UPPER);
    private static final Calibration CAL = new Calibration(0.0, 1.0, 0.0);

    @Test
    void firstFire_CusumCrossesAndLevelGateOpen_FiresAtTheCrossing() {
        double[] series = {1.0, 1.0, 1.0, 1.0};
        Optional<CusumLevelAlert.Fire> fire = CusumLevelAlert.firstFire(series, series, CAL, CFG);
        assertTrue(fire.isPresent());
        assertEquals(2, fire.get().index());
        assertEquals(1.0, fire.get().fireValue());
        assertEquals(3.0, fire.get().sPlus());   // S+ = max(0, 2 + 1) at the firing step
    }

    @Test
    void firstFire_LevelGateClosed_DoesNotFireEvenWhenCusumCrosses() {
        double[] cusum = {1.0, 1.0, 1.0, 1.0};       // S+ would cross h=2 at index 2
        double[] level = {-1.0, -1.0, -1.0, -1.0};   // but the level gate (>= L=0) is never open
        assertTrue(CusumLevelAlert.firstFire(cusum, level, CAL, CFG).isEmpty());
    }

    @Test
    void firstFire_NaNFireValueIsAGap_CarriesAccumulatorsAndNeverFiresOnThatBar() {
        // S+ reaches 2 at index 1 (not yet > h). The NaN at index 2 is a gap: no fire there and S+
        // carries unchanged at 2; the next 1.0 (index 3) pushes S+ to 3 and crosses h. Without the
        // gap-carry, the NaN would have corrupted S+ and the fire index would differ.
        double[] series = {1.0, 1.0, Double.NaN, 1.0, 1.0};
        Optional<CusumLevelAlert.Fire> fire = CusumLevelAlert.firstFire(series, series, CAL, CFG);
        assertTrue(fire.isPresent());
        assertEquals(3, fire.get().index());
    }

    @Test
    void countFires_PerWindowReArm_LetsEachWindowFireOnceAndDebouncesWithin() {
        // Two windows of three 1.0 bars each: S+ crosses inside each window once (debounce), and the
        // window boundary re-arms the detector -> exactly two fires.
        double[] series = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
        int[] windows = {0, 0, 0, 1, 1, 1};
        assertEquals(2, CusumLevelAlert.countFires(series, series, windows, CAL, CFG));
    }

    @Test
    void countFires_SingleWindow_DebouncesToOneFire() {
        double[] series = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
        int[] oneWindow = {0, 0, 0, 0, 0, 0};
        assertEquals(1, CusumLevelAlert.countFires(series, series, oneWindow, CAL, CFG));
    }

    @Test
    void firstFire_QuietSeries_NeverFires_IsACensoredMiss() {
        double[] series = {0.0, 0.0, 0.0, 0.0};
        assertTrue(CusumLevelAlert.firstFire(series, series, CAL, CFG).isEmpty());
        assertFalse(CusumLevelAlert.firstFire(series, series, CAL, CFG).isPresent());
    }
}
