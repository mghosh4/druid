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
    public void setQueryRuntimeEstimate(String queryType, long queryTime)
    {
        if (estimateQueryRuntime == true) {
            int numSamples = 3;
            float alpha = 2/(1+numSamples);
            MutablePair<Long, Long> runtime = this.queryRuntimeEstimateTable.get(queryType);
            if (runtime != null) {
                long oldEstimate = runtime.lhs;
                long oldSamples = runtime.rhs;

                runtime.lhs = Long.valueOf((long)(runtime.lhs*(1-alpha)) + (long)(queryTime*alpha));
                runtime.rhs = runtime.rhs + 1L;
                this.queryRuntimeEstimateTable.put(queryType, runtime);

                log.info("Set done queryRuntimeEstimateTable queryType %s, queryTime %d oldEstimate %d, oldSamples %d, newEstimate %d, newSamples %d",
                        queryType, queryTime, oldEstimate, oldSamples, runtime.lhs, runtime.rhs);
            } else {
                log.info("Error: query type %s not found in queryRuntimeEstimateTable ", queryType);
            }
        }
    }

    @Override
    public long getQueryRuntimeEstimate(String queryType)
    {
        if (estimateQueryRuntime == true) {
            if (this.queryRuntimeEstimateTable.get(queryType) != null) {
                long estimate = this.queryRuntimeEstimateTable.get(queryType).lhs;
                long numSamples = this.queryRuntimeEstimateTable.get(queryType).rhs;
                if (numSamples != 0) {
                    return estimate;
                } else {
                    return 0L;
                }
            } else {
                return 0L;
            }
        }
        else{
            return 0L;
        }
    }
}
