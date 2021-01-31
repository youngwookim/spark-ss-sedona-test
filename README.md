```
$ mvn clean package

```

```
$ nc -lk 9999
40.7128,-74.0060
40.7128,-74.0060

$ nc -lk 8888

```

```
cd $SPARK_HOME
$ ./bin/spark-submit   --class com.example.SparkApp   --master local[*]   /Users/ywkim/workspace/spark-ss-sedona-demo/target/spark-ss-sedona-1.0.0-jar-with-dependencies.jar localhost 9999 localhost 8888
```

```
......

Caused by: org.apache.spark.sql.AnalysisException: Stream-stream join without equality predicate is not supported;;
Join Inner,  **org.apache.spark.sql.geosparksql.expressions.ST_Contains$**

......
```