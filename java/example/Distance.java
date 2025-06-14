package com.example.my;

import org.apache.flink.table.functions.ScalarFunction;


public class Distance extends ScalarFunction {
    public static final String NAME = "calculateDistanceKm";

    // Equirectangular approximation to calculate distance in km between two points 
    public float eval(float lat1, float lon1, float lat2, float lon2) {
        float EARTH_RADIUS = 6371;
        float lat1Rad = (float) Math.toRadians(lat1);
        float lat2Rad = (float) Math.toRadians(lat2);
        float lon1Rad = (float) Math.toRadians(lon1);
        float lon2Rad = (float) Math.toRadians(lon2);

        float x = (float) ((lon2Rad - lon1Rad) * Math.cos((lat1Rad + lat2Rad) / 2));
        float y = (lat2Rad - lat1Rad);
        float distance = (float) (Math.sqrt(x * x + y * y) * EARTH_RADIUS);

        return distance;
    }

}