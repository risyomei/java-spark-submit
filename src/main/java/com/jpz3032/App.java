package com.jpz3032;

import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args1 ) {
        // prepare arguments to be passed to
        // org.apache.spark.deploy.yarn.Client object
        String[] args = new String[] {
                // the name of your application
                "--name",
                "Java-Calling-Spark-Test",

                // path to your application's JAR file
                // required in yarn-cluster mode
                "--jar",
                "/opt/spark/examples/jars/spark-examples_2.12-3.2.1-line-p3.jar",

                // name of your application's main class (required)
                "--class",
                "org.apache.spark.examples.SparkPi",

                // argument 1 to your Spark program (SparkFriendRecommendation)
                "--arg",
                "1000",
        };

        // create a Hadoop Configuration object
        Configuration config = new Configuration();

        // identify that you will be using Spark as YARN mode
        System.setProperty("SPARK_YARN_MODE", "true");

        // create an instance of SparkConf object
        SparkConf sparkConf = new SparkConf();

        // create ClientArguments, which will be passed to Client
        // ClientArguments cArgs = new ClientArguments(args, sparkConf);
        ClientArguments cArgs = new ClientArguments(args);

        // create an instance of yarn Client client
        // Client client = new Client(cArgs, config, sparkConf);
        Client client = new Client(cArgs, sparkConf, null);

        // submit Spark job to YARN
        client.run();
    }
}
