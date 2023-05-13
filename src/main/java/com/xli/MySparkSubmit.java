package com.xli;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkSubmit;

public class MySparkSubmit {
    public static void main( String[] args1 ) {
         String[] submitArgs = new String[]{
            // "--verbose",
            "--class", "org.apache.spark.examples.SparkPi",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "/opt/spark/examples/jars/spark-examples_2.12-3.2.1-customzied.jar",
            "1000"
        };

        System.setProperty("HADOOP_CONF_DIR", "/etc/hadoop/conf");
        System.setProperty("HADOOP_HOME", "/opt/cloudera/parcels/CDH/lib/hadoop");
        System.setProperty("HADOOP_COMMON_HOME", "/opt/cloudera/parcels/CDH/lib/hadoop");
        System.setProperty("HADOOP_HDFS_HOME", "/opt/cloudera/parcels/CDH/lib/hadoop-hdfs");
        System.setProperty("HADOOP_YARN_HOME", "/opt/cloudera/parcels/CDH/lib/hadoop-yarn");
        System.setProperty("HADOOP_MAPRED_HOME", "/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce");
        System.setProperty("SPARK_HOME", "/opt/spark");
        System.setProperty("SPARK_DIST_CLASSPATH", "/etc/hadoop/conf:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/.//*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/./:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/.//*");
        SparkSubmit.main(submitArgs);
    }
}

/*
# Confirmed Working

export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_HOME=/opt/spark
export SPARK_DIST_CLASSPATH="/etc/hadoop/conf:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/.//*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/./:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/libexec/../../hadoop-yarn/.//*" 

java -cp "target/sparktest-1.0-SNAPSHOT.jar:/opt/spark/conf:/opt/spark/jars/*:/etc/hadoop/conf:/opt/cloudera/parcels/CDH/lib/hadoop/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*:/opt/cloudera/parcels/CDH/lib/hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop-yarn/*" com.xli.MySparkSubmit

*/

