package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.model

case class Driver( driverId: String,  driverName: String,  driverAddress: String,  driverEmail: String) {
  println(s" driverId= $driverId driverName= $driverName driverAddress= $driverAddress driverEmail= $driverEmail")
}
