package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.service

import java.time.LocalDate

import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.converter.Conversions.{DriversExtension, TripExtension}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class TaxiService(tripsInputFile: String, driversInputFile: String) {
  private val sparkConf = new SparkConf().setAppName("Trip App").setMaster("local[*]")
  private val sc = new SparkContext(sparkConf)
  private val tripsRdd: RDD[String] = sc.textFile(tripsInputFile)
  private val driversRdd: RDD[String] = sc.textFile(driversInputFile)

  def findNumberOfLines():Long = tripsRdd.count()

  def findNumberOfTripsSameCityLongerThenX(city: String, length: Int): Long = {
    tripsRdd.map(l => l.convertToTrip())
      .filter(t => t.tripTargetCity.equalsIgnoreCase(city) && t.tripLength >= length)
      .count()
  }

  def totalSumOfTripsToSameCity(city: String): Double = {
    tripsRdd.map(l => l.convertToTrip())
      .filter(t => t.tripTargetCity.equalsIgnoreCase(city))
      .map(t => t.tripLength)
      .sum()
  }


  def top3WithMaxTripLengthOnGivenDay(date: LocalDate): Array[String] = {
    val drivers: RDD[(String, String)]
    = driversRdd.map(l => l.convertToDriver()).map(d => Tuple2(d.driverId, d.driverName))

    tripsRdd.map(l => l.convertToTrip())
      .filter(t => t.tripDateTime.isEqual(date))
      .map(t =>  Tuple2(t.driverId, t.tripLength))
      .reduceByKey((l1, l2) => l1 + l2)
      .join(drivers)
      .values
      .sortBy(t => t._1, false:Boolean, 1)
      .map(p => p._2)
      .take(3)

  }
}


