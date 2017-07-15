package io.druid.query;

import com.metamx.emitter.EmittingLogger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mainakghosh on 7/7/2017.
 */
public class DecayedQueryRuntimeEstimator extends DefaultQueryRuntimeEstimator
{
    private static final EmittingLogger log = new EmittingLogger(DecayedQueryRuntimeEstimator.class);

    @Override
    public void setQueryRuntimeEstimate(String queryType, long queryDurationMillis, long querySegmentTime){
        long numIgnoredEstimates = ignoredEstimates.get(queryType);
        if(numIgnoredEstimates > numEstimateValuesToIgnore) {
            if (estimateQueryRuntime == true) {
                float numSamples = 19;
                float alpha = 2/(1+numSamples);
                ConcurrentHashMap<Long, MutablePair<Long, Long>> durationMap = this.queryRuntimeEstimateTable.get(queryType);
                if (durationMap != null) {
                    MutablePair<Long, Long> runtime = durationMap.get(queryDurationMillis);
                    long oldEstimate = runtime.lhs;
                    long oldSamples = runtime.rhs;

                    runtime.lhs = Long.valueOf((long)(runtime.lhs*(1-alpha)) + (long)(querySegmentTime*alpha));
//          Long smallerDurationEstimate = durationMap.get(queryDurationMillis-1000).lhs;
//          if(runtime.lhs <= smallerDurationEstimate){
//            runtime.lhs = smallerDurationEstimate + 1;
//          }
                    runtime.rhs = runtime.rhs + 1L;
                    durationMap.put(queryDurationMillis, runtime);

                    log.info("Set done queryRuntimeEstimateTable queryType %s, queryDuration %d, queryTime %d oldEstimate %d, oldSamples %d, newEstimate %d, newSamples %d",
                            queryType, queryDurationMillis, querySegmentTime, oldEstimate, oldSamples, runtime.lhs, runtime.rhs);
                } else {
                    log.info("Error: query type %s not found in queryRuntimeEstimateTable ", queryType);
                }
            }
        }
        else{
            log.info("Ignoring estimate for %s, count ", queryType, numIgnoredEstimates);
            ignoredEstimates.put(queryType, numIgnoredEstimates+1);
        }
    }

    @Override
    public long getQueryRuntimeEstimate(String queryType, long queryDurationMillis){
        if (estimateQueryRuntime == true) {
            if (this.queryRuntimeEstimateTable.get(queryType) != null) {
                long estimate = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).lhs;
                long numSamples = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).rhs;
                if (numSamples != 0) {
                    return estimate;
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        }
        else{
            return 0;
        }
    }
}
