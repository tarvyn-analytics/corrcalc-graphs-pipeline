package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservationTest.observation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ThinningObserverTest {

    @Test
    void onObservation_EveryThird_ForwardsOneInThree() {
        List<PipelineObservation> kept = new ArrayList<>();
        ThinningObserver thin = new ThinningObserver(kept::add, 3);

        for (int i = 0; i < 7; i++) {
            thin.onObservation(observation(0.01 * i, 0.0, 0.0, false));
        }

        assertEquals(2, kept.size(), "7 observations, every 3rd -> indices 3 and 6");
    }

    @Test
    void onObservation_EveryOne_ForwardsAll() {
        List<PipelineObservation> kept = new ArrayList<>();
        ThinningObserver thin = new ThinningObserver(kept::add, 1);
        thin.onObservation(observation(0.0, 0.0, 0.0, false));
        thin.onObservation(observation(0.0, 0.0, 0.0, false));
        assertEquals(2, kept.size());
    }

    @Test
    void constructor_RejectsNullDelegateAndNonPositiveN() {
        assertThrows(IllegalArgumentException.class, () -> new ThinningObserver(null, 1));
        assertThrows(IllegalArgumentException.class, () -> new ThinningObserver(o -> { }, 0));
    }
}
