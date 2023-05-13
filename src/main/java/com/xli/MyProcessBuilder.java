package com.xli;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.List;
import java.util.ArrayList;

public class MyProcessBuilder {
    public static void main(String[] args) {

        ExecutorService executor = Executors.newFixedThreadPool(20);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            Future<?> future = executor.submit(() -> {
                try {
                    ProcessBuilder processBuilder = new ProcessBuilder(
                        "/opt/spark/bin/spark-submit",
                        "--class", "org.apache.spark.examples.SparkPi",
                        "--master", "yarn",
                        "/opt/spark/examples/jars/spark-examples_2.12-3.2.1-customzied.jar",
                        "1000"
                    );
                    processBuilder.inheritIO(); 
                    Process process = processBuilder.start();
                    process.waitFor();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });

            futures.add(future);
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();
    }
}

// java -cp "target/sparktest-1.0-SNAPSHOT.jar" com.xli.MyProcessBuilder