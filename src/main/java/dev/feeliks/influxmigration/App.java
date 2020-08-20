package dev.feeliks.influxmigration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * Loading data to InfluxDB
 */
public class App {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Logger logger = LoggerFactory.getLogger(App.class);

        final int NoThreads = 2;
        // Runtime.getRuntime().availableProcessors();
        final ExecutorService executorService = Executors.newFixedThreadPool(NoThreads);
        Path directory = Paths.get(
                Objects.requireNonNull(args[0]));

        List<Callable<String>> callableList = new ArrayList<>();
        try (Stream<Path> files = Files.walk(directory)
                .filter(f -> f.toFile().isFile() && f.toFile().getName().endsWith(".csv"))
                .sorted()) {
            files.forEach(line -> callableList.add(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Path file = Objects.requireNonNull(line);
                    try {
                        logger.info("Pooling {}", file.toFile().getName());
                        DataLoad.fromCsv(file);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                    return file.toFile().getName();
                }
            }));
        }

        List<Future<String>> futures = executorService.invokeAll(callableList);
        for (Future<String> f : futures) {
            logger.info("{} completed!", f.get());
        }
        logger.info("Preparing shutdown..");
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        logger.info("Done!");
    }

}