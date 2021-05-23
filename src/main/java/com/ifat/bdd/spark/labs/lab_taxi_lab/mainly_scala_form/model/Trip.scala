package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_scala_form.model

import java.time.LocalDate

case class Trip( driverId: String,  tripTargetCity: String,  tripLength: Int,  tripDateTime: LocalDate) {
  println(s"driverId=$driverId tripTargetCity=$tripTargetCity tripLength=$tripLength date=$tripDateTime ")
}
