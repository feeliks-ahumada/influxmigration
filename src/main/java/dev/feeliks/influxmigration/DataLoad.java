package dev.feeliks.influxmigration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Stream;

import dev.feeliks.influxmigration.utilities.Partition;

public class DataLoad {
    private static final Logger logger = LoggerFactory.getLogger(DataLoad.class);

    private DataLoad(){}

    /**
     * @param file File path
     */
    public static void fromCsv(Path file) {
        List<List<String>> rows = getRows(file);
        rows.forEach(Persistence::toInflux);
    }

    private static List<List<String>> getRows(Path file) {
        List<List<String>> rows = new ArrayList<>();
        int chunkSize = 100;
        try (Stream<String> lines = Files.lines(file)) {
            rows = lines.skip(1).map(DataLoad::lineProtocol).
                    collect(partitioned(chunkSize));
        } catch (IOException ioException)
        {
            logger.error(ioException.getMessage());
        }
        return rows;
    }

    public static String lineProtocol(String line) {
        String[] cols = line.replace("\"","").split(",");

        ZonedDateTime timestamp = getZonedDateTime(cols);

        String format;
        try {
            format = String.format("Wind,station=%s direction=\"%s\",speed=%s %d",
                    cols[17],
                    cols[15],
                    cols[16],
                    timestamp.toEpochSecond()
            );
        } catch (Exception e) {
            format = String.format("fails,m=%s value=%s %d",cols[17],cols[0],timestamp.toEpochSecond());
            logger.error(e.getMessage());
        }
        return format;
    }

    private static ZonedDateTime getZonedDateTime(String[] cols) {
        ZonedDateTime timestamp;

        try {
            timestamp = ZonedDateTime.of(Integer.parseInt(cols[1]),
                    Integer.parseInt(cols[2]), Integer.parseInt(cols[3]),
                    Integer.parseInt(cols[4]), 0, 0, 0,
                    ZoneId.of("UTC"));
        } catch (NumberFormatException e) {
            timestamp = ZonedDateTime.now();
            logger.error(e.getMessage());
        }
        return timestamp;
    }

    private static <T> Collector<T, List<T>, List<List<T>>> partitioned(int chunkSize) {
        return Collector.of(
                ArrayList::new,
                List::add,
                (a,b) -> { a.addAll(b); return a; },
                a -> Partition.ofSize(a, chunkSize),
                Collector.Characteristics.UNORDERED
        );
    }
}
