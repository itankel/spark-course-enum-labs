package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.main;

import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.service.TaxiService;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.time.LocalDate;
import java.time.Month;

public class TaxiLabMain {
    static {
        System.setProperty("hadoop.home.dir", "C:\\Hadoop\\winutils\\");
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

    }

    public static void main(String[] args) {

        TaxiService taxiService = new TaxiService("data/taxi/trips.txt", "data/taxi/drivers.txt");
        System.out.println("findNumbersOfLines returns >>>> " + taxiService.findNumberOfLines());
        System.out.println("amount of trips to Boston longer than 10 Km >>>>>>" +
                taxiService.findNumberOfTripsSameCityLongerThenX("Boston", 10));
        System.out.println("Sum of all kilometers trips to Boston >>>>>>" +
                taxiService.totalSumOfTripsToSameCity("Boston"));

        System.out.println("Top 3 drivers with max total length in specified "
                + LocalDate.of(2016, Month.FEBRUARY, 20) + " day >>>>>> "
                + taxiService.top3WithMaxTripLengthOnGivenDay(
                        LocalDate.of(2016, Month.FEBRUARY, 20)));

    }
}
