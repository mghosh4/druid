package io.druid.server.coordination.broker.tasks;

import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordinator.helper.DruidCoordinatorReplicatorHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lexu on 4/29/17.
 */
public class TargetSelect {

    private static final EmittingLogger log = new EmittingLogger(TargetSelect.class);

    public static String compareAndSelect(HashMap<String, Double> currentMap, HashMap<String, Double> goalMap){

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
                goalArr[counter] = goalMap.get(key);
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

        //refactoring goal array
        for (int i = 0; i < goalArr.length; i++){
            refactoredGoalArr[i] = goalArr[i] * maxFactor;
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

        return IDToKey.get(targetID);

    }
}
