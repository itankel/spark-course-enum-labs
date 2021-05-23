package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Data
@Builder
@Getter
public class Drivers {
    private String driverId;
    private String driverName;
    private String driverAddress;
    private String driverEmail;
}
