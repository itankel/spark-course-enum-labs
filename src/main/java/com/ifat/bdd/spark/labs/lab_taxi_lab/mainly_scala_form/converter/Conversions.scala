package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.converter

import java.time.{LocalDate, Month}

import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.model.{Driver, Trip}

object Conversions {

  implicit class TripExtension(line: String) {
    def convertToTrip(): Trip = {
      val lineArgs: Array[String] = line.split(" ")
      val month = Month.values().find(m => m.name().toUpperCase.startsWith(lineArgs(4).trim.toUpperCase)).get.toString
      val dateOf: LocalDate = LocalDate.of(lineArgs(8).trim.toInt, Month.valueOf(month), lineArgs(5).toInt)
      Trip.apply(lineArgs(0).trim, lineArgs(1).trim, lineArgs(2).trim.toInt, dateOf)
    }
  }

  implicit class DriversExtension(line: String) {
    def convertToDriver(): Driver = {
      val lineArgs: Array[String] = line.split(",")
      Driver.apply(lineArgs(0).trim, lineArgs(1).trim, lineArgs(2).trim, lineArgs(3).trim)
    }
  }

}
