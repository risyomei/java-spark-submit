package com.xli;

import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

/**
 * Hello world!
 *
 */
public class MyYarnClient {
    public static void main( String[] args1 ) {
        // prepare arguments to be passed to
        // org.apache.spark.deploy.yarn.Client object
        String[] args = new String[] {
                // the name of your application
                // "--name",
                // "Java-Calling-Spark-Test",

                // path to your application's JAR file
                // required in yarn-cluster mode
                "--jar",
                "/opt/spark/examples/jars/spark-examples_2.12-3.2.1-customzied.jar",

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
        System.setProperty("HADOOP_CONF_DIR", "/etc/hadoop/conf");
        System.setProperty("HADOOP_HOME", "/opt/cloudera/parcels/CDH/lib/hadoop");
        System.setProperty("SPARK_HOME", "/opt/spark");

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

/*
# Not Working Yet

export SPARK_HOME=/opt/spark
export SPARK_DIST_CLASSPATH="/etc/hadoop/conf:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/.//*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/./:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/.//*" 

java -cp "target/sparktest-1.0-SNAPSHOT.jar:/opt/spark/conf:/opt/spark/jars/*:/etc/hadoop/conf:/opt/cloudera/parcels/CDH/lib/hadoop/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*:/opt/cloudera/parcels/CDH/lib/hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop-yarn/*" com.xli.MyYarnClient



Caused by: io.netty.channel.AbstractChannel$AnnotatedSocketException: Invalid argument: /0.0.3.232:0
Caused by: java.net.SocketException: Invalid argument
        at sun.nio.ch.Net.connect0(Native Method)
        at sun.nio.ch.Net.connect(Net.java:482)
        at sun.nio.ch.Net.connect(Net.java:474)
        at sun.nio.ch.SocketChannelImpl.connect(SocketChannelImpl.java:647)
        at io.netty.util.internal.SocketUtils$3.run(SocketUtils.java:91)
        at io.netty.util.internal.SocketUtils$3.run(SocketUtils.java:88)
        at java.security.AccessController.doPrivileged(Native Method)
        at io.netty.util.internal.SocketUtils.connect(SocketUtils.java:88)
        at io.netty.channel.socket.nio.NioSocketChannel.doConnect(NioSocketChannel.java:315)
        at io.netty.channel.nio.AbstractNioChannel$AbstractNioUnsafe.connect(AbstractNioChannel.java:248)
        at io.netty.channel.DefaultChannelPipeline$HeadContext.connect(DefaultChannelPipeline.java:1342)
        at io.netty.channel.AbstractChannelHandlerContext.invokeConnect(AbstractChannelHandlerContext.java:548)
        at io.netty.channel.AbstractChannelHandlerContext.connect(AbstractChannelHandlerContext.java:533)
        at io.netty.channel.AbstractChannelHandlerContext.connect(AbstractChannelHandlerContext.java:517)
        at io.netty.channel.DefaultChannelPipeline.connect(DefaultChannelPipeline.java:978)
        at io.netty.channel.AbstractChannel.connect(AbstractChannel.java:265)
        at io.netty.bootstrap.Bootstrap$3.run(Bootstrap.java:250)
        at io.netty.util.concurrent.AbstractEventExecutor.safeExecute(AbstractEventExecutor.java:164)
        at io.netty.util.concurrent.SingleThreadEventExecutor.runAllTasks(SingleThreadEventExecutor.java:469)
        at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:500)
        at io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:986)
        at io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
        at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
        at java.lang.Thread.run(Thread.java:748)
*/