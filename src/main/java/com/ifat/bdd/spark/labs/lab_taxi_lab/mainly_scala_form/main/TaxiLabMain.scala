package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.main

import java.time.LocalDate

import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.service.TaxiService
import org.apache.log4j.{Level, Logger}

object TaxiLabMain {
  def init() {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\winutils\\")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    init()

    val taxiService = new TaxiService("data/taxi/trips.txt",
      "data/taxi/drivers.txt")
    println("findNumbersOfLines returns >>>> " + taxiService.findNumberOfLines())
    println("amount of trips to Boston longer than 10 Km >>>>>>" +
      taxiService.findNumberOfTripsSameCityLongerThenX("Boston", 10))
    println("Sum of all kilometers trips to Boston >>>>>>" +
      taxiService.totalSumOfTripsToSameCity("Boston"))

    println("Top 3 drivers with max total length in specified "
      + LocalDate.of(2016, 2, 20) + " day >>>>>> "
      + taxiService.top3WithMaxTripLengthOnGivenDay(
      LocalDate.of(2016, 2, 20)).mkString(","))

  }

}
