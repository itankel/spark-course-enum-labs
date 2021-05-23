package com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.converter;

import com.ifat.bdd.spark.labs.lab_taxi_lab.mainly_java_form.model.Trip;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;

public class TripsInfoUtil {
    enum TripsParams{
        DRIVER_ID,
        TARGET_CITY,
        TRIP_LENGTH,DAY_OF_WEEK,MONTH,DAY_OF_MONTH,START_TIME,TIME_ZONE,YEAR
    }
    public static Trip ConvertToTrips(String rowData){
        String[] row = rowData.split(" ");
        System.out.println("row ==> "+ Arrays.toString(row));
        String driverId=row[TripsParams.DRIVER_ID.ordinal()].trim();
        System.out.println("driverId = " + driverId);
        String tripTargetCity=row[TripsParams.TARGET_CITY.ordinal()].trim();
        System.out.println("tripTargetCity = " + tripTargetCity);
        int tripLength=Integer.parseInt(row[TripsParams.TRIP_LENGTH.ordinal()]);
        System.out.println("tripLength = " + tripLength);
        String dayOfWeek = row[TripsParams.DAY_OF_WEEK.ordinal()].trim();
        System.out.println("dayOfWeek = " + dayOfWeek);
        String month = row[TripsParams.MONTH.ordinal()].trim();// string Feb need to convert
        System.out.println("month = " + month);
        int dayOfMonth =Integer.parseInt(row[TripsParams.DAY_OF_MONTH.ordinal()]);
        System.out.println("dayOfMonth = " + dayOfMonth);

        String[] time = row[TripsParams.START_TIME.ordinal()].trim().split(":");
        int hours=Integer.parseInt(time[0]);
        System.out.println("hours = " + hours);
        int min=Integer.parseInt(time[1]);
        System.out.println("min = " + min);
        int seconds = Integer.parseInt(time[2]);
        System.out.println("seconds = " + seconds);

        String timeZone=row[TripsParams.TIME_ZONE.ordinal()].trim();
        System.out.println("timeZone = " + timeZone);
        int year=Integer.parseInt(row[TripsParams.YEAR.ordinal()].trim());
        System.out.println("year = " + year);

        Month mon = Arrays.stream(Month.values())
                .filter(m -> m.name().substring(0, 3).equalsIgnoreCase(month))
                .findFirst()
                .orElseThrow(RuntimeException::new);
       return  Trip.builder().driverId(driverId)
                .tripTargetCity(tripTargetCity)
                .tripLength(tripLength)
                .tripDateTime(LocalDateTime.of(year, mon,dayOfMonth,hours,min,seconds)).build();
    }

}
