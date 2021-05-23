package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.service;

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
import java.util.logging.Logger;

public class TaxiService {
    private final String applicationName = "Taxi App";
    private SparkConf sparkConf;
    private JavaSparkContext sc;
    private JavaRDD<String> tripsRdd;
    private JavaRDD<String> driversRdd;

    public TaxiService(String tripsInputFile, String driversInputFile) {
        sparkConf = new SparkConf().setAppName(applicationName).setMaster("local[*]");
        sc = new JavaSparkContext(sparkConf);
        tripsRdd = sc.textFile(tripsInputFile);
        driversRdd = sc.textFile(driversInputFile);
    }

    public long findNumberOfLines() {
        return tripsRdd.count();
    }

    public long findNumberOfTripsSameCityLongerThenX(final String city, final int length) {
        return tripsRdd.map(TripsInfoUtil::ConvertToTrips)
                .filter(t -> t.getTripTargetCity().equalsIgnoreCase(city) && t.getTripLength() >= length)
                .count();
    }

    public Double totalSumOfTripsToSameCity(final String city) {
        return tripsRdd.map(TripsInfoUtil::ConvertToTrips)
                .filter(t -> t.getTripTargetCity().equalsIgnoreCase(city))
                .mapToDouble(Trip::getTripLength)
                .sum();
    }

    public List<String> top3WithMaxTripLengthOnGivenDay(LocalDate date) {
        JavaPairRDD<String, String> drivers = driversRdd.map(DriverUtil::DriversInfoConverter)
                .mapToPair(t -> new Tuple2<>(t.getDriverId(), t.getDriverName()));

        return
                tripsRdd.map(TripsInfoUtil::ConvertToTrips)
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
