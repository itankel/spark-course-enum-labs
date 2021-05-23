package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.time.LocalDateTime;

@Builder
@Data
@Getter
public class Trip {
    private String driverId;
    private String tripTargetCity;
    private int tripLength;
    private LocalDateTime tripDateTime;
}
