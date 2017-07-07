package io.druid.query;

import com.metamx.emitter.EmittingLogger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mainakghosh on 7/7/2017.
 */
public class DefaultQueryRuntimeEstimator implements QueryRuntimeEstimator
{
    String[] queryTypes = {Query.TIMESERIES, Query.TOPN, Query.GROUP_BY};

    private static final EmittingLogger log = new EmittingLogger(DefaultQueryRuntimeEstimator.class);

    protected volatile ConcurrentHashMap<String, MutablePair<Long, Long>> queryRuntimeEstimateTable = new ConcurrentHashMap<>();
    // map maintains the history of query allocations to different HNs
    protected volatile static boolean estimateQueryRuntime = false;

    @Override
    public void startQueryRuntimeEstimation()
    {
        this.estimateQueryRuntime = true;

        for (String qt : queryTypes)
        {
            this.queryRuntimeEstimateTable.put(qt, new MutablePair<>(0L, 0L));
        }
    }

    @Override
    public void setQueryRuntimeEstimate(String queryType, long queryTime)
    {
        log.info("Setting queryRuntimeEstimate table for queryType %s, queryDuration %d, queryTime %d", queryType, queryTime);
        if (estimateQueryRuntime == true) {
            MutablePair<Long, Long> runtime = this.queryRuntimeEstimateTable.get(queryType);
            if (runtime != null) {
                long oldEstimate = runtime.lhs;
                long oldSamples = runtime.rhs;

                runtime.lhs = runtime.lhs + queryTime;
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
                long totalRuntime = this.queryRuntimeEstimateTable.get(queryType).lhs;
                long numSamples = this.queryRuntimeEstimateTable.get(queryType).rhs;
                if (numSamples != 0) {
                    return totalRuntime / numSamples;
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

    @Override
    public void clearQueryRuntimeEstimate()
    {
        for (String qt : queryTypes)
        {
            this.queryRuntimeEstimateTable.put(qt, new MutablePair<>(0L, 0L));
        }
    }
}
