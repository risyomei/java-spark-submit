package com.xli;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.launcher.InProcessLauncher;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class MySparkLauncher {
    public static void main( String[] args1 ) throws Exception  {

        ExecutorService executor = Executors.newFixedThreadPool(20); 
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            Future<?> future = executor.submit(() -> {
                try {
                    SparkAppHandle handle = new SparkLauncher()
                            .setAppResource("/opt/spark/examples/jars/spark-examples_2.12-3.2.1-customzied.jar")
                            .setMainClass("org.apache.spark.examples.SparkPi")
                            .setMaster("yarn")
                            .addAppArgs("1000")
                            .setVerbose(true)
                            .startApplication();

                    while ( handle.getState() != SparkAppHandle.State.FINISHED && 
                            handle.getState() != SparkAppHandle.State.FAILED &&
                            handle.getState() != SparkAppHandle.State.KILLED &&
                            handle.getState() != SparkAppHandle.State.LOST) {
                        System.out.println(handle.getState());
                        Thread.sleep(1000);
                    }

                    System.out.println(handle.getState());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            futures.add(future);
        }

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();
    }
}


/*
# Confirmed Working

export SPARK_HOME=/opt/spark

java -cp "target/sparktest-1.0-SNAPSHOT.jar:/opt/spark/conf:/opt/spark/jars/*:/etc/hadoop/conf:/opt/cloudera/parcels/CDH/lib/hadoop/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*:/opt/cloudera/parcels/CDH/lib/hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop-yarn/*" com.xli.MySparkLauncher

*/