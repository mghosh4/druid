package io.druid.query;

import com.metamx.emitter.EmittingLogger;

import javax.inject.Inject;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mainakghosh on 7/7/2017.
 */
public class DefaultQueryRuntimeEstimator implements QueryRuntimeEstimator
{
    List<String> queryTypes = new ArrayList<>(Arrays.asList(Query.TIMESERIES, Query.TOPN, Query.GROUP_BY));
    public static long segmentDurationMillis = 600000;  // Code assumes segment size is 1minute. This will create 60 1sec buckets in the map

    private static final EmittingLogger log = new EmittingLogger(DefaultQueryRuntimeEstimator.class);

    // query runtime estimate. Maps query type to query duration bucket (60buckets, 1sec in size). Duration bucket maps
    // duration to a tuple of (query runtime sum, number of samples over which that sum was taken). Number of
    // samples is used to computed the query runtime mean.
    protected volatile static ConcurrentHashMap<String, ConcurrentHashMap<Long, MutablePair<Long, Long>>>
            queryRuntimeEstimateTable = new ConcurrentHashMap<>();

    protected volatile static boolean estimateQueryRuntime = true;
    // These variables are used to ignore the initial estimates as they are not accurate.
    protected static int numEstimateValuesToIgnore = 5; // this threshold is not strictly followed due to multi-threading
    protected volatile static ConcurrentHashMap<String, Long> ignoredEstimates = new ConcurrentHashMap<>();

    @Inject
    public DefaultQueryRuntimeEstimator()
    {
        for(String qt : queryTypes){
            ConcurrentHashMap<Long, MutablePair<Long, Long>> temp = new ConcurrentHashMap<>();
            for(long i=0L; i<=segmentDurationMillis; i=i+1000){
                MutablePair <Long, Long> p = new MutablePair<>(0L, 0L);
                temp.put(i, p);
            }
            this.queryRuntimeEstimateTable.put(qt, temp);
            this.ignoredEstimates.put(qt, 0L);
        }
    }

    @Override
    public void startQueryRuntimeEstimation()
    {
        this.estimateQueryRuntime = true;
    }

    @Override
    public void setQueryRuntimeEstimate(String queryType, long queryDurationMillis, long queryTime)
    {
        if (!queryTypes.contains(queryType))
            return;

        long numIgnoredEstimates = 0;
        if (ignoredEstimates.containsKey(queryType))
            numIgnoredEstimates = ignoredEstimates.get(queryType);

        if(numIgnoredEstimates > numEstimateValuesToIgnore) {
            log.info("Setting queryRuntimeEstimate table for queryType %s, queryDuration %d, queryTime %d", queryType, queryDurationMillis, queryTime);
            if (estimateQueryRuntime == true) {
                ConcurrentHashMap<Long, MutablePair<Long, Long>> durationMap = this.queryRuntimeEstimateTable.get(queryType);
                if (durationMap != null) {
                    MutablePair<Long, Long> runtime = durationMap.get(queryDurationMillis);
                    long oldEstimate = runtime.lhs;
                    long oldSamples = runtime.rhs;

                    runtime.lhs = runtime.lhs + queryTime;
                    runtime.rhs = runtime.rhs + 1L;
                    durationMap.put(queryDurationMillis, runtime);

                    log.info("Set done queryRuntimeEstimateTable queryType %s, queryDuration %d, queryTime %d oldEstimate %d, oldSamples %d, newEstimate %d, newSamples %d",
                            queryType, queryDurationMillis, queryTime, oldEstimate, oldSamples, runtime.lhs, runtime.rhs);
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
    public long getQueryRuntimeEstimate(String queryType, long queryDurationMillis)
    {
        if (estimateQueryRuntime == true) {
            if (this.queryRuntimeEstimateTable.get(queryType) != null) {
                long totalRuntime = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).lhs;
                long numSamples = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).rhs;
                if (numSamples != 0) {
                    return totalRuntime / numSamples;
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

    @Override
    public void clearQueryRuntimeEstimate()
    {
        for(ConcurrentHashMap.Entry<String, ConcurrentHashMap<Long, MutablePair<Long, Long>>> e1 : queryRuntimeEstimateTable.entrySet()){
            for(ConcurrentHashMap.Entry<Long, MutablePair<Long, Long>> e2 : e1.getValue().entrySet()){
                ConcurrentHashMap<Long, MutablePair<Long, Long>> temp = new ConcurrentHashMap<>();
                temp.put(e2.getKey(), new MutablePair<Long, Long>(0L, 0L));
                queryRuntimeEstimateTable.put(e1.getKey(), temp);
            }
        }
    }
}
