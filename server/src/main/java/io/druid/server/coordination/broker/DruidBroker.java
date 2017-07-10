/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import io.druid.client.ServerInventoryView;
import io.druid.client.ServerView;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Self;
import io.druid.query.Query;
import io.druid.query.MutablePair;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.broker.tasks.PeriodicPollHistoricalLoad;
import io.druid.server.coordination.broker.tasks.PeriodicPollRoutingTable;
import io.druid.timeline.DataSegment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@ManageLifecycle
public class DruidBroker
{
  private final DruidNode self;
  private final ServiceAnnouncer serviceAnnouncer;
  private final ServerDiscoveryFactory serverDiscoveryFactory;
  private final HttpClient httpClient;
  private final ServerInventoryView inventoryView;

  private final ScheduledExecutorService pool;
  private volatile boolean started = false;

  // getafix routing table. Maps segment ID to <HN, allocation> map
  private volatile Map<String, Map<String, Long>> routingTable;
  // query runtime estimate. Maps query type to query duration bucket (60buckets, 1sec in size). Duration bucket maps
  // duration to a tuple of (query runtime sum, number of samples over which that sum was taken). Number of
  // samples is used to computed the query runtime mean.
  private volatile ConcurrentHashMap<String, ConcurrentHashMap<Long, MutablePair<Long, Long>>> queryRuntimeEstimateTable = new ConcurrentHashMap<>();
  // map maintains the history of query allocations to different HNs
  private volatile ConcurrentHashMap<String, Long> hnQueryTimeAllocation = new ConcurrentHashMap<>();
  private volatile static boolean estimateQueryRuntime = false;
  private static final EmittingLogger log = new EmittingLogger(DruidBroker.class);

  //QueryTime distribution data structures
  String loadingPath = "/proj/DCSQ/mghosh4/druid/estimation/";
  String[] fullpaths = {loadingPath+"groupby.cdf", loadingPath+"timeseries.cdf", loadingPath+"topn.cdf"};

  HashMap<String, ArrayList<Double>> percentileCollection = new HashMap<String, ArrayList<Double>>();
  HashMap<String, HashMap<Double, Double>> histogramCollection = new HashMap<String, HashMap<Double, Double>>();
  ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> allocationTable = new ConcurrentHashMap<>();
  String[] queryTypes = {Query.TIMESERIES, Query.TOPN, Query.GROUP_BY};

  @Inject
  public DruidBroker(
      final ServerInventoryView serverInventoryView,
      final @Self DruidNode self,
      final ServiceAnnouncer serviceAnnouncer,
      final ServerDiscoveryFactory serverDiscoveryFactory,
      @Global HttpClient httpClient
  )  {
    this.self = self;
    this.serviceAnnouncer = serviceAnnouncer;
    this.serverDiscoveryFactory = serverDiscoveryFactory;
    this.httpClient = httpClient;
    this.inventoryView = serverInventoryView;

    serverInventoryView.registerSegmentCallback(
        MoreExecutors.sameThreadExecutor(),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            serviceAnnouncer.announce(self);
            return ServerView.CallbackAction.UNREGISTER;
          }
        }
    );

    this.pool = Executors.newScheduledThreadPool(2);
    this.routingTable = new HashMap<>();

    //populate all query time distribution data structures
    for(int i = 0 ; i < queryTypes.length; i++){
      String key = queryTypes[i];
      HashMap<Double, Double> histogram = new HashMap<Double, Double>();
      ArrayList<Double> percentile = null;
      try {
        percentile = loadAndParse(fullpaths[i], histogram);
      } catch (IOException e) {
        e.printStackTrace();
        break;
      }
      percentileCollection.put(key, percentile);
      histogramCollection.put(key, histogram);
    }

    // initialize queryRuntimeEstimateTable
    long segmentDurationMillis = 600000;  // Code assumes segment size is 1minute. This will create 60 1sec buckets in the map
    List<String> queryType = new ArrayList<String>();
    //queryType.add(Query.TIME_BOUNDARY);
    queryType.add(Query.TIMESERIES);
    queryType.add(Query.TOPN);
    queryType.add(Query.GROUP_BY);
    //queryType.add(Query.SEARCH);
    //queryType.add(Query.SELECT);

    for(String qt : queryType){
      ConcurrentHashMap<Long, MutablePair<Long, Long>> temp = new ConcurrentHashMap<>();
      for(long i=0L; i<=segmentDurationMillis; i=i+1000){
        MutablePair <Long, Long> p = new MutablePair<>(0L, 0L);
        temp.put(i, p);
      }
      this.queryRuntimeEstimateTable.put(qt, temp);
    }
  }

  public void startQueryRuntimeEstimation(){
    this.estimateQueryRuntime = true;
  }

  public void setDecayedQueryRuntimeEstimate(String queryType, long queryDurationMillis, long querySegmentTime){
    if (estimateQueryRuntime == true) {
      float numSamples = 3;
      float alpha = 2/(1+numSamples);
      ConcurrentHashMap<Long, MutablePair<Long, Long>> durationMap = this.queryRuntimeEstimateTable.get(queryType);
      if (durationMap != null) {
        MutablePair<Long, Long> runtime = durationMap.get(queryDurationMillis);
        long oldEstimate = runtime.lhs;
        long oldSamples = runtime.rhs;

        runtime.lhs = Long.valueOf((long)(runtime.lhs*(1-alpha)) + (long)(querySegmentTime*alpha));
        runtime.rhs = runtime.rhs + 1L;
        durationMap.put(queryDurationMillis, runtime);

        log.info("Set done queryRuntimeEstimateTable queryType %s, queryDuration %d, queryTime %d oldEstimate %d, oldSamples %d, newEstimate %d, newSamples %d",
                queryType, queryDurationMillis, querySegmentTime, oldEstimate, oldSamples, runtime.lhs, runtime.rhs);
      } else {
        log.info("Error: query type %s not found in queryRuntimeEstimateTable ", queryType);
      }
    }
  }

  public long getDecayedQueryRuntimeEstimate(String queryType, long queryDurationMillis){
    if (estimateQueryRuntime == true) {
      if (this.queryRuntimeEstimateTable.get(queryType) != null) {
        long estimate = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).lhs;
        long numSamples = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).rhs;
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

  public void setQueryRuntimeEstimate(String queryType, long queryDurationMillis, long queryTime){
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

  public long getQueryRuntimeEstimate(String queryType, long queryDurationMillis){
    if (estimateQueryRuntime == true) {
      if (this.queryRuntimeEstimateTable.get(queryType) != null) {
        long totalRuntime = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).lhs;
        long numSamples = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).rhs;
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

  public void clearQueryRuntimeEstimate(){
    for(ConcurrentHashMap.Entry<String, ConcurrentHashMap<Long, MutablePair<Long, Long>>> e1 : queryRuntimeEstimateTable.entrySet()){
      for(ConcurrentHashMap.Entry<Long, MutablePair<Long, Long>> e2 : e1.getValue().entrySet()){
        ConcurrentHashMap<Long, MutablePair<Long, Long>> temp = new ConcurrentHashMap<>();
        temp.put(e2.getKey(), new MutablePair<Long, Long>(0L, 0L));
        queryRuntimeEstimateTable.put(e1.getKey(), temp);
      }
    }
  }

  public void printQueryRuntimeEstimateTable(){
    log.info("Query runtime estimate table %s", queryRuntimeEstimateTable.toString());
  }

/*
  public void setQueryRuntimeEstimate(String queryType, long queryDurationMillis, long queryTime){
    log.info("Setting queryRuntimeEstimate table for queryType %s, queryDuration %d, queryTime %d", queryType, queryDurationMillis, queryTime);
    ConcurrentHashMap<Long, MutablePair<Long, Long>> durationMap = this.queryRuntimeEstimateTable.get(queryType);
    if (durationMap != null){
      MutablePair<Long, Long> runtime = durationMap.get(queryDurationMillis);
      long oldEstimate = runtime.lhs;
      long oldSamples = runtime.rhs;

      runtime.lhs = runtime.lhs + queryTime;
      runtime.rhs = runtime.rhs + 1L;
      durationMap.put(queryDurationMillis, runtime);

      log.info("Set done queryRuntimeEstimateTable queryType %s, queryDuration %d, queryTime %d oldEstimate %d, oldSamples %d, newEstimate %d, newSamples %d",
              queryType, queryDurationMillis, queryTime, oldEstimate, oldSamples, runtime.lhs, runtime.rhs);
    }
    else{
      log.info("Error: query type %s not found in queryRuntimeEstimateTable ", queryType);
    }
  }

  public long getQueryRuntimeEstimate(String queryType, long queryDurationMillis){
    if(this.queryRuntimeEstimateTable.get(queryType) != null) {
      long totalRuntime = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).lhs;
      long numSamples = this.queryRuntimeEstimateTable.get(queryType).get(queryDurationMillis).rhs;
      if (numSamples != 0) {
        return totalRuntime / numSamples;
      } else {
        return 0L;
      }
    }
    else{
      return 0L;
    }
  }
*/
  public ConcurrentHashMap<String, Long> getHNQueryTimeAllocation(){
    return this.hnQueryTimeAllocation;
  }

  public void printHNQueryTimeAllocationTable(){
    log.info("HN query time allocation table %s", hnQueryTimeAllocation.toString());
  }
  public void clearHNQueryTimeAllocationTable(){
    for(Map.Entry<String, Long> entry : this.hnQueryTimeAllocation.entrySet()){
      hnQueryTimeAllocation.put(entry.getKey(), 1L);
    }
  }

  @LifecycleStart
  public void start()
  {
    synchronized (self) {
      if(started) {
        return;
      }
      started = true;

      // Scheduled Tasks
      //pool.scheduleWithFixedDelay(new PeriodicPollRoutingTable(this, serverDiscoveryFactory, httpClient), 0, 20, TimeUnit.SECONDS);
      //pool.scheduleWithFixedDelay(new PeriodicPollHistoricalLoad(inventoryView, httpClient), 0, 20, TimeUnit.SECONDS);
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (self) {
      if (!started) {
        return;
      }
      serviceAnnouncer.unannounce(self);
      started = false;
    }
  }

  public boolean acceptableQueryType(String queryType)
  {
    return Arrays.asList(queryTypes).contains(queryType);
  }

  public HashMap<String, ArrayList<Double>> getPercentileCollection() {
    return percentileCollection;
  }

  public HashMap<String, HashMap<Double, Double>> getHistogramCollection() {
    return histogramCollection;
  }

  public void setAllocationTable(ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> allocationTable) {
    this.allocationTable = allocationTable;
  }

  public void clearAllocationTable() {
    this.allocationTable.clear();
  }

  public ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> getAllocationTable() { return allocationTable;  }

  public synchronized Map<String, Map<String, Long>> getRoutingTable()
  {
    return routingTable;
  }

  private void printRoutingTable(final Map<String, Map<String, Long>> routingTable){
      for(Map.Entry<String, Map<String, Long>> entry : routingTable.entrySet()){
          //log.info("Segment [%s]:", entry.getKey().getIdentifier());
          log.info("Segment [%s]:", entry.getKey());
          for(Map.Entry<String, Long> e: entry.getValue().entrySet()){
              //log.info("[%s]: [%s]", e.getKey().getHost(), e.getValue());
              log.info("HN [%s] : Value [%s]", e.getKey(), e.getValue());
          }
      }
  }

  public synchronized void setRoutingTable(Map<String, Map<String, Long>> routingTable)
  {
    this.routingTable = routingTable;
    log.info("Received routing table");
    printRoutingTable(routingTable);
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public ArrayList<Double> loadAndParse(String filename, HashMap<Double, Double> histogram) throws IOException {
    ArrayList<Double> percentileArr = new ArrayList<Double>();

    /*********************************************************************/
    /* http://stackoverflow.com/questions/5819772/java-parsing-text-file */
    FileReader input = new FileReader(filename);
    BufferedReader bufRead = new BufferedReader(input);
    String myLine = null;

    while ( (myLine = bufRead.readLine()) != null)
    {
      String[] array = myLine.split("\\s+");
      double querytime = Double.parseDouble(array[0]);
      double percentile = Double.parseDouble(array[1]);
      percentileArr.add(percentile);
      histogram.put(percentile, querytime);
    }

    /*********************************************************************/
    return percentileArr;
  }
}
