package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.converter;

import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.converter.DriverUtil;
import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.converter.TripsInfoUtil;
import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.model.Trip;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;
import java.util.List;


public class TaxiUtils {

    public static long findNumberOfLines() {
        SparkConf sparkConf = new SparkConf().setAppName("Taxi App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.textFile("data/taxi/trips.txt");
        return rdd.count();
    }

    public static long findNumberOfTripsSameCityLongerThenX(String city, int length) {
        SparkConf sparkConf = new SparkConf().setAppName("Taxi App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.textFile("data/taxi/trips.txt");
        return rdd.map(TripsInfoUtil::ConvertToTrips)
                .filter(t -> t.getTripTargetCity().equalsIgnoreCase(city) && t.getTripLength() >= length)
                .count();
    }

    public static Double totalSumOfTripsToSameCity(String city) {
        SparkConf sparkConf = new SparkConf().setAppName("Taxi App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.textFile("data/taxi/trips.txt");
        return rdd.map(TripsInfoUtil::ConvertToTrips)
                .filter(t -> t.getTripTargetCity().equalsIgnoreCase(city))
                .mapToDouble(Trip::getTripLength)
                .sum();
    }

    public static List<String> top3WithMaxTripLengthOnGivenDay(LocalDate date) {
        SparkConf sparkConfd = new SparkConf().setAppName("Taxi App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConfd);
        JavaRDD<String> rddDrivers = sc.textFile("data/taxi/drivers.txt");
        JavaPairRDD<String, String> drivers = rddDrivers.map(DriverUtil::DriversInfoConverter)
                .mapToPair(t -> new Tuple2<>(t.getDriverId(), t.getDriverName()));

        JavaRDD<String> rdd = sc.textFile("data/taxi/trips_changed.txt");
        return
                rdd.map(TripsInfoUtil::ConvertToTrips)
                        .filter(t -> t.getTripDateTime().toLocalDate().isEqual(date))
                        .mapToPair(t -> new Tuple2<>(t.getDriverId(), t.getTripLength()))
                        .reduceByKey(Integer::sum)
                        .join(drivers)
                        .values()
                        .sortBy(integerStringTuple2 -> integerStringTuple2._1, false, 1)
                        .map(p -> p._2)
                        .take(3);

    }


}
