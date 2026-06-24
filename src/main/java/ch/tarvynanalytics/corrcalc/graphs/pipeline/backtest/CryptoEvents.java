package ch.tarvynanalytics.corrcalc.graphs.pipeline.backtest;

import java.time.LocalDate;
import java.util.List;

/**
 * The eight crypto regime events of the n=8 mechanism test, in the spike's order — a port of
 * {@code crypto_universe.EVENTS}. This is the universe over which the pipeline reproduces
 * {@code crypto_lead_table.csv}.
 */
public final class CryptoEvents {

    private CryptoEvents() {
    }

    /** The eight events, in lead-table row order. */
    public static final List<CryptoEvent> ALL = List.of(
            new CryptoEvent("covid_crash_2020",
                    LocalDate.of(2020, 3, 11), LocalDate.of(2020, 3, 14),
                    LocalDate.of(2020, 1, 5), LocalDate.of(2020, 2, 19)),
            new CryptoEvent("may2021_selloff",
                    LocalDate.of(2021, 5, 18), LocalDate.of(2021, 5, 23),
                    LocalDate.of(2021, 3, 13), LocalDate.of(2021, 4, 27)),
            new CryptoEvent("china_mining_ban_2021",
                    LocalDate.of(2021, 6, 18), LocalDate.of(2021, 6, 22),
                    LocalDate.of(2021, 4, 13), LocalDate.of(2021, 5, 28)),
            new CryptoEvent("nov2021_cycle_top",
                    LocalDate.of(2021, 11, 9), LocalDate.of(2021, 11, 16),
                    LocalDate.of(2021, 9, 4), LocalDate.of(2021, 10, 19)),
            new CryptoEvent("luna_terra_2022",
                    LocalDate.of(2022, 5, 8), LocalDate.of(2022, 5, 14),
                    LocalDate.of(2022, 3, 3), LocalDate.of(2022, 4, 17)),
            new CryptoEvent("ftx_collapse_2022",
                    LocalDate.of(2022, 11, 7), LocalDate.of(2022, 11, 11),
                    LocalDate.of(2022, 9, 2), LocalDate.of(2022, 10, 17)),
            new CryptoEvent("usdc_svb_2023",
                    LocalDate.of(2023, 3, 9), LocalDate.of(2023, 3, 13),
                    LocalDate.of(2023, 1, 2), LocalDate.of(2023, 2, 16)),
            new CryptoEvent("yen_carry_unwind_2024",
                    LocalDate.of(2024, 8, 4), LocalDate.of(2024, 8, 6),
                    LocalDate.of(2024, 5, 30), LocalDate.of(2024, 7, 14)));

    /** Looks up an event by name, or throws if unknown. */
    public static CryptoEvent byName(String name) {
        return ALL.stream()
                .filter(e -> e.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown crypto event [" + name + "]"));
    }
}
