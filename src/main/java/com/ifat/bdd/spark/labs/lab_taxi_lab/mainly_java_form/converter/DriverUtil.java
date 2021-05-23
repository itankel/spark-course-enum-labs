package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.converter;

import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.model.Drivers;

import java.util.Arrays;

public class DriverUtil {
    public static Drivers DriversInfoConverter(String rowData){
        String[] row = rowData.split(",");
        System.out.println("row ==> "+ Arrays.toString(row));
        String driverId=row[0].trim();
        System.out.println("driverId = " + driverId);
        String driverName=row[1].trim();
        System.out.println("driverName = " + driverName);
        String driverAddress=row[2].trim();
        System.out.println("driverAddress = " + driverAddress);

        String driverEmail = row[3].trim();
        System.out.println("driverEmail = " + driverEmail);

        return  Drivers.builder().driverId(driverId)
                .driverName(driverName)
                .driverAddress(driverAddress)
                .driverEmail(driverEmail).build();
    }

}
