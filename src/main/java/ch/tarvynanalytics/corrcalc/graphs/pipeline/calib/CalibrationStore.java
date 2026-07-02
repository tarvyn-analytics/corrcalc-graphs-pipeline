package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

/**
 * Persists {@link CalibrationArtifact}s as human-inspectable JSON — the Jackson IO at the CGP edge
 * (the lib never sees persistence; zero-dep line, H2 design §2.2). Package-private: the public
 * surface is the records plus the {@link CalibrationSource} seam ({@link CalibrationSources} wires
 * loading for the CLI). Non-finite doubles (the {@code NaN} de-fusion-uncalibrated marker) survive
 * the round trip as Jackson's quoted non-numeric tokens.
 */
final class CalibrationStore {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);   // instants as ISO-8601 strings

    private CalibrationStore() {
    }

    /**
     * Reads an artifact from {@code json}, validating it through the record constructor (an
     * unsupported {@code schemaVersion} or a malformed window rejects the file).
     *
     * @param json the artifact file
     * @return the artifact
     * @throws UncheckedIOException if the file cannot be read or parsed
     */
    static CalibrationArtifact load(Path json) {
        try {
            return MAPPER.readValue(json.toFile(), CalibrationArtifact.class);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read calibration artifact [" + json + "]", e);
        }
    }

    /**
     * Writes {@code artifact} to {@code json} (pretty-printed, overwriting).
     *
     * @param artifact the artifact to persist
     * @param json     the destination file
     * @throws UncheckedIOException if the file cannot be written
     */
    static void save(CalibrationArtifact artifact, Path json) {
        if (artifact == null) {
            throw new IllegalArgumentException("artifact must not be null");
        }
        try {
            MAPPER.writeValue(json.toFile(), artifact);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write calibration artifact [" + json + "]", e);
        }
    }
}
