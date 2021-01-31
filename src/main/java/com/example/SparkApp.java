package com.example;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;

public class SparkApp {

	public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

		if (args.length < 4) {
			System.exit(1);
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);

		String host1 = args[2];
		int port1 = Integer.parseInt(args[3]);

		SparkSession spark = SparkSession.builder()
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator")
				.appName("JavaStructuredNetworkWordCount").getOrCreate();

		GeoSparkSQLRegistrator.registerAll(spark);

		Dataset<Row> pts = spark.readStream().format("socket").option("host", host).option("port", port).load();

		pts.isStreaming();
		pts.printSchema();

		Dataset<Row> poly = spark.readStream().format("socket").option("host", host1).option("port", port1).load();

		pts.createOrReplaceTempView("pts");
		poly.createOrReplaceTempView("poly");

		Dataset<Row> fenced = spark.sql(
				"select * from poly, pts WHERE ST_Contains(ST_PolygonFromText(poly.value,','), ST_PointFromText(pts.value,','))");

		StreamingQuery query = fenced.writeStream()
				.format("console").start();

		query.awaitTermination();
	}
}
