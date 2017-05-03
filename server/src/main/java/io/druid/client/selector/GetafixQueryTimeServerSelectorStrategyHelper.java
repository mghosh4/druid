package io.druid.client.selector;

import com.metamx.emitter.EmittingLogger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


public class GetafixQueryTimeServerSelectorStrategyHelper {

    private static final EmittingLogger log = new EmittingLogger(GetafixQueryTimeServerSelectorStrategyHelper.class);


    public static double selectRandomQueryTime(HashMap<Double, Double> histogram, ArrayList<Double> percentileArr){
        double queryTime = 0.0;
        //http://stackoverflow.com/questions/9724404/random-floating-point-double-in-inclusive-range
        double dice = Math.random() < 0.5 ? ((1-Math.random()) * (1-0) + 0) : (Math.random() * (1-0) + 0);
        //binary serach in percentileArr
        int left = 0;
        int right = percentileArr.size()-1;
        int cur = (left+right)/2;
        while(right-left>1){
            if(percentileArr.get(cur)<=dice){
                left = cur;
            }
            else{
                right = cur;
            }
            cur = (left+right)/2;
        }
        log.info("Select left percentile [%s]: [%s]", left, percentileArr.get(left));
        queryTime = histogram.get(percentileArr.get(left));
        log.info("Estimated Query Time [%s]", queryTime);
        return queryTime;
    }

    public static String compareAndSelect(HashMap<String, Double> currentMap, Map<String, Long> goalMap){

        HashMap<String , Integer> keyToID = new HashMap<String, Integer>();
        HashMap<Integer, String> IDToKey = new HashMap<Integer, String>();
        double[] currentArr = new double[currentMap.size()];
        double[] goalArr = new double[goalMap.size()];
        double[] refactoredGoalArr = goalArr = new double[goalArr.length];
        double maxFactor = 0.0;


        if(currentMap.size()!=goalMap.size()){
            log.info("current map [%s] and goal map [%s] have different sizes", currentMap.size(), goalMap.size());
        }
        int counter = 0;
        for(Map.Entry<String, Double> entry : currentMap.entrySet()){
            if(goalMap.containsKey(entry.getKey())){
                //populate all data structure
                String key = entry.getKey();
                currentArr[counter] = currentMap.get(key);
                goalArr[counter] = (double)goalMap.get(key);
                keyToID.put(key, counter);
                IDToKey.put(counter, key);
                counter++;
            }
            else{
                log.info("currentMap contains key [%s] that doesnt exist in goal Map", entry.getKey());
            }
        }

        //find the max factor possible
        int peakID = 0;
        for(int i = 0; i < currentArr.length; i++){
            double localFactor = currentArr[i]/(double)goalArr[i];
            if(localFactor > maxFactor) {
                maxFactor = localFactor;
                peakID = i;
            }
        }
        log.info("max multiplication factor [%s]", maxFactor);
        log.info("current array:");
        for(int i =0; i<currentArr.length;i++){
            log.info("[%s]", currentArr[i]);
        }
        log.info("original goal array:");
        for(int i =0; i<goalArr.length;i++){
            log.info("[%s]", goalArr[i]);
        }
        //refactoring goal array
        for (int i = 0; i < goalArr.length; i++){
            refactoredGoalArr[i] = goalArr[i] * maxFactor;
        }
        log.info("refactoredGoalArr:");
        for(int i =0; i<refactoredGoalArr.length;i++){
            log.info("[%s]", refactoredGoalArr[i]);
        }

        //find the one furthest from its goal
        double maxDistance = 0.0;
        int targetID = peakID;
        for(int i=0; i<goalArr.length; i++ ){
            double distance = refactoredGoalArr[i]-currentArr[i];
            if(distance<0.0){
                log.error("Negative distance at index [%s]: [%s]-[%s]=[%s], maps to server [%s]", i, refactoredGoalArr[i],currentArr[i],distance,IDToKey.get(i));
            }
            if(distance>maxDistance){
                maxDistance = distance;
                targetID = i;
            }
        }
        log.info("max distance [%s]", maxDistance);
        return IDToKey.get(targetID);

    }
}
