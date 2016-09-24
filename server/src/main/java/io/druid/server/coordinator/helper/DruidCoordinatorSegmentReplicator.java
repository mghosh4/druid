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

package io.druid.server.coordinator.helper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DruidCoordinatorSegmentReplicator implements DruidCoordinatorHelper
{
  private final DruidCoordinator coordinator;

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorSegmentReplicator.class);
  private static final String assignedCount = "assignedCount";
  private static final String droppedCount = "droppedCount";
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  private final HttpClient httpClient;
  private final ServerDiscoveryFactory serverDiscoveryFactory;

  private static final double MIN_THRESHOLD = 5;

  public DruidCoordinatorSegmentReplicator(
      DruidCoordinator coordinator,
      HttpClient httpClient,
      ServerDiscoveryFactory factory
  )
  {
    this.coordinator = coordinator;
    this.httpClient = httpClient;
    this.serverDiscoveryFactory = factory;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    //log.info("Starting replication. Getting Segment Popularity");
    final CoordinatorStats stats = new CoordinatorStats();
    HashMap<DataSegment, Number> insertList = new HashMap<>();
    HashMap<DataSegment, Number> removeList = new HashMap<>();

    // Acquire Query Workload in the last window
    Map<DataSegment, Long> segments = Maps.newHashMap();
    calculateSegmentCounts(segments);

    // Calculate the popularity map
    HashMap<DataSegment, Double> weightedAccessCounts = coordinator.getWeightedAccessCounts();
    calculateWeightedAccessCounts(params, segments, weightedAccessCounts, removeList);
    coordinator.setWeightedAccessCounts(weightedAccessCounts);

    // Calculate replication based on popularity
    //calculateAdaptiveReplication(params, weightedAccessCounts, insertList, removeList);
    calculateBestFitReplication(params, weightedAccessCounts, insertList, removeList);

    // Manage replicas
    manageReplicas(params, insertList, removeList, stats);

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  private List<String> getBrokerURLs()
  {
    String brokerService = TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME;
    ServerDiscoverySelector selector = serverDiscoveryFactory.createSelector(brokerService);
    try {
      selector.start();
    }
    catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    Collection<Server> brokers = selector.getAll();

    List<String> uris = new ArrayList<>(brokers.size());
    for (Server broker : brokers) {
      // Should use threads to fetch in parallel from all brokers
      URI uri = null;
      try {
        uri = new URI(
            broker.getScheme(),
            null,
            broker.getAddress(),
            broker.getPort(),
            "/druid/broker/v1/segments",
            null,
            null
        );
      }
      catch (URISyntaxException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      log.info("URI [%s]", uri.toString());

      uris.add(uri.toString());
    }

    log.info("Number of Broker Servers [%d]", brokers.size());

    return uris;
  }

  private void calculateSegmentCounts(Map<DataSegment, Long> segmentCounts)
  {
    //log.info("Starting replication. Getting Segment Popularity");
    List<String> urls = getBrokerURLs();

    ExecutorService pool = Executors.newFixedThreadPool(urls.size());
    List<Future<Map<DataSegment, Long>>> futures = new ArrayList<>();

    for (final String url : urls) {
      futures.add(pool.submit(new Callable<Map<DataSegment, Long>>()
      {
        @Override
        public Map<DataSegment, Long> call()
        {
          Map<DataSegment, Long> segments;
          try {
            StatusResponseHolder response = httpClient.go(
                new Request(
                    HttpMethod.GET,
                    new URL(url)
                ),
                responseHandler
            ).get();

            if (!response.getStatus().equals(HttpResponseStatus.OK)) {
              throw new ISE(
                  "Error while querying[%s] status[%s] content[%s]",
                  url,
                  response.getStatus(),
                  response.getContent()
              );
            }

            log.info("Response Length [%d]", response.getContent().length());

            segments = jsonMapper.readValue(response.getContent(), new TypeReference<Map<DataSegment, Long>>()
            {
            });
          }
          catch (Exception e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
          }
				    
				    /*for (DataSegment segment:segments)
				    {
				        log.info("Segment Received [%s]", segment.getIdentifier());
				    }*/

          return segments;
        }
      }));
    }

    for (Future<Map<DataSegment, Long>> future : futures) {
      try {
        Map<DataSegment, Long> segments = future.get();
        for (Map.Entry<DataSegment, Long> entry : segments.entrySet()) {
          Long prevVal = segmentCounts.getOrDefault(entry.getKey(), 0L);
          segmentCounts.put(entry.getKey(), entry.getValue() + prevVal);
        }
      } catch (InterruptedException | ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
		
		/*for (Entry<DataSegment> entry : segmentCounts.entrySet())
		{
			if (entry != null)
				log.info("Segment Received [%s] Count [%d]", entry.getElement().getIdentifier(), entry.getCount());
		}*/
  }

  private void calculateWeightedAccessCounts(
      DruidCoordinatorRuntimeParams params,
      Map<DataSegment, Long> segments,
      HashMap<DataSegment, Double> weightedAccessCounts,
      HashMap<DataSegment, Number> removeList
  )
  {
    //log.info("Calculating Weighted Access Counts for Segments");

    // Handle those segments which are in Coordinator's map but not in segments collected from query
		for (Map.Entry<DataSegment, Double> entry : weightedAccessCounts.entrySet()) {
      if (!segments.containsKey(entry.getKey())) {
        weightedAccessCounts.put(entry.getKey(), 0.5 * entry.getValue());
      }
		}

    for (Map.Entry<DataSegment, Long> entry : segments.entrySet()) {
      DataSegment segment = entry.getKey();
      long segmentCount = entry.getValue();

			if (!weightedAccessCounts.containsKey(segment)) {
        weightedAccessCounts.put(segment, (double) segmentCount);
			} else {
				double popularity = weightedAccessCounts.get(segment);
				weightedAccessCounts.put(segment, (double) segmentCount + 0.5 * popularity);
			}
    }

    // Remove segments with counts less than a threshold from weightedAccessCounts. Also add it to removeList
    Iterator<Map.Entry<DataSegment, Double>> it = weightedAccessCounts.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<DataSegment, Double> entry = it.next();
      //log.info("Segment Received [%s] Count [%f]", entry.getKey().getIdentifier(), entry.getValue().doubleValue());

      if (entry.getValue() < MIN_THRESHOLD) {
        DataSegment segment = entry.getKey();
        removeList.put(segment, params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier()));
        it.remove();
      }
    }
  }

  private void calculateAdaptiveReplication(
      DruidCoordinatorRuntimeParams params,
      HashMap<DataSegment, Double> weightedAccessCounts,
      HashMap<DataSegment, Number> insertList,
      HashMap<DataSegment, Number> removeList
  )
  {
    //log.info("Calculating Replication for Segments");
    int historicalNodeCount = 0;
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
      historicalNodeCount += serverQueue.size();
		}

    double totalWeightCount = 0;
		for (Number number : weightedAccessCounts.values()) {
      totalWeightCount += number.doubleValue();
		}

    for (Map.Entry<DataSegment, Double> entry : weightedAccessCounts.entrySet()) {
      DataSegment segment = entry.getKey();
      int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());

      double newReplicationFactor = Math.ceil(entry.getValue() * historicalNodeCount / totalWeightCount);

			if (newReplicationFactor == 0) {
        removeList.put(segment, -1);
			} else if (newReplicationFactor < totalReplicantsInCluster) {
        removeList.put(segment, totalReplicantsInCluster - newReplicationFactor);
			} else if (newReplicationFactor > totalReplicantsInCluster) {
        insertList.put(segment, newReplicationFactor - totalReplicantsInCluster);
			}
    }
  }

  private void calculateBestFitReplication(
      DruidCoordinatorRuntimeParams params,
      HashMap<DataSegment, Double> weightedAccessCounts,
      HashMap<DataSegment, Number> insertList,
      HashMap<DataSegment, Number> removeList
  )
  {
    //log.info("Calculating Best Fit Replication for Segments");
    int historicalNodeCount = 0;
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
      historicalNodeCount += serverQueue.size();
		}

    double totalWeightCount = 0;
		for (Number number : weightedAccessCounts.values()) {
      totalWeightCount += number.doubleValue();
		}

    log.info("Total Weight Count [%f]", totalWeightCount);

    int slotsperhn = (int) Math.ceil(totalWeightCount / historicalNodeCount);

    int[] nodeCapacities = new int[historicalNodeCount];
    for (int i = 0; i < historicalNodeCount; i++) {
      nodeCapacities[i] = slotsperhn;
    }

    PriorityQueue<Tuple> maxheap = new PriorityQueue<Tuple>(10, new Comparator<Tuple>()
    {
      public int compare(Tuple o1, Tuple o2)
      {
        return -Double.compare(o1.weight, o2.weight);
      }
    });

    for (Map.Entry<DataSegment, Double> entry : weightedAccessCounts.entrySet()) {
      maxheap.add(new Tuple(entry.getKey(), entry.getValue()));
    }

    Multiset<DataSegment> expectedCount = HashMultiset.create();
    while (maxheap.peek() != null) {
      Tuple candidate = maxheap.poll();
      //log.info("Chosen Segment [%s] [%f]", candidate.segment.getIdentifier(), candidate.weight);

      int valleft = bestFit(candidate.weight, nodeCapacities);
      expectedCount.add(candidate.segment);
      if (valleft > 0) {
        maxheap.add(new Tuple(candidate.segment, valleft));
      }
    }

    for (Entry<DataSegment> entry : expectedCount.entrySet()) {
      int totalReplicantsInCluster = params.getSegmentReplicantLookup()
                                           .getTotalReplicants(entry.getElement().getIdentifier());
      int newReplicationFactor = entry.getCount();

      log.info(
          "Replication Decision for [%s]: Current [%d] Required [%d]",
          entry.getElement().getIdentifier(),
          totalReplicantsInCluster,
          newReplicationFactor
      );

			if (newReplicationFactor < totalReplicantsInCluster) {
        removeList.put(entry.getElement(), totalReplicantsInCluster - newReplicationFactor);
			} else if (newReplicationFactor > totalReplicantsInCluster) {
        insertList.put(entry.getElement(), newReplicationFactor - totalReplicantsInCluster);
			}
    }
  }

  private int bestFit(double val, int nodeCapacities[])
  {
    int mincapleftafterfill = Integer.MAX_VALUE;
    int minvalleftafterfill = Integer.MAX_VALUE;
    int minfitindex = 0;
    int minspillindex = 0;
    int counter = 0;
    boolean fits = false;
    for (int capacity : nodeCapacities) {
      if (val <= capacity) {
        fits = true;
        int leftafterfill = (int) (capacity - val);
        if (leftafterfill < mincapleftafterfill) {
          mincapleftafterfill = leftafterfill;
          minfitindex = counter;
        }
      } else {
        int leftafterfill = (int) (val - capacity);
        if (leftafterfill < minvalleftafterfill) {
          minvalleftafterfill = leftafterfill;
          minspillindex = counter;
        }
      }
      counter += 1;
    }
    if (fits) {
      nodeCapacities[minfitindex] = mincapleftafterfill;
      return 0;
    } else {
      nodeCapacities[minspillindex] = 0;
      return minvalleftafterfill;
    }
  }

  private void manageReplicas(
      DruidCoordinatorRuntimeParams params,
      HashMap<DataSegment, Number> insertList,
      HashMap<DataSegment, Number> removeList,
      CoordinatorStats stats
  )
  {
    //log.info("Managing Replicas by inserting and removing replicas for relevant data segments");

    List<ServerHolder> serverHolderList = new ArrayList<ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
      serverHolderList.addAll(serverQueue);
		}

    if (serverHolderList.size() == 0) {
      log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
      return;
    }

    final List<String> tierNameList = Lists.newArrayList(params.getDruidCluster().getTierNames());
    if (tierNameList.size() == 0) {
      log.makeAlert("Cluster has multiple tiers! Check your cluster configuration!").emit();
      return;
    }
    final String tier = tierNameList.get(0);

    final DateTime referenceTimestamp = params.getBalancerReferenceTimestamp();
    final BalancerStrategy strategy = params.getBalancerStrategyFactory().createBalancerStrategy(referenceTimestamp);

    for (Map.Entry<DataSegment, Number> entry : insertList.entrySet()) {
      String id = entry.getKey().getIdentifier();
      DataSegment segment = null;
      for (DataSegment candidate : coordinator.getAvailableDataSegments()) {
        //log.info("Comparing candidate: %s to incumbent %s", candidate.getIdetifier(), id);
        if (candidate.getIdentifier().equals(id)) {
          segment = candidate;
          break;
        }
      }

			if (segment == null) {
        continue;
			}

      CoordinatorStats assignStats = assign(
          params.getReplicationManager(),
          tier,
          strategy,
          serverHolderList,
          segment,
          entry.getValue().intValue()
      );
      stats.accumulate(assignStats);
    }

    for (Map.Entry<DataSegment, Number> entry : removeList.entrySet()) {
      String id = entry.getKey().getIdentifier();
      DataSegment segment = null;
      for (DataSegment candidate : coordinator.getAvailableDataSegments()) {
        if (candidate.getIdentifier().equals(id)) {
          segment = candidate;
          break;
        }
      }

			if (segment == null) {
        continue;
			}

      int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());
      if (totalReplicantsInCluster <= 0) {
        continue;
      }

      int numReplicantsToRemove = entry.getValue().intValue() == -1
                                  ? totalReplicantsInCluster
                                  : entry.getValue().intValue();
      CoordinatorStats dropStats = drop(
          params.getReplicationManager(),
          tier,
          serverHolderList,
          segment,
          numReplicantsToRemove
      );
      stats.accumulate(dropStats);
    }
  }

  private CoordinatorStats assign(
      final ReplicationThrottler replicationManager,
      final String tier,
      final BalancerStrategy strategy,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment,
      final int numReplicantsToAdd
  )
  {
    log.info("Insert Segment [%s] [%d]", segment.getIdentifier(), numReplicantsToAdd);

    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToTieredStat(assignedCount, tier, 0);

    int numReplicants = numReplicantsToAdd;
    while (numReplicants > 0) {
      final ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);

      if (holder == null) {
        log.warn(
            "Not enough [%s] servers or node capacity to assign segment[%s]!",
            tier,
            segment.getIdentifier()
        );
        break;
      }

      replicationManager.registerReplicantCreation(
          tier, segment.getIdentifier(), holder.getServer().getHost()
      );

      holder.getPeon().loadSegment(
          segment,
          new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
              replicationManager.unregisterReplicantCreation(
                  tier,
                  segment.getIdentifier(),
                  holder.getServer().getHost()
              );
            }
          }
      );

      log.info("Inserted Segment [%s]", segment.getIdentifier());

      stats.addToTieredStat(assignedCount, tier, 1);
      --numReplicants;
    }

    return stats;
  }

  private CoordinatorStats drop(
      final ReplicationThrottler replicationManager,
      final String tier,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment,
      final int numReplicantsToRemove
  )
  {
    log.info("Remove Segment [%s] [%d]", segment.getIdentifier(), numReplicantsToRemove);
    CoordinatorStats stats = new CoordinatorStats();

    // Pick the server which has the maximum number of segments for load balance
    Map<Number, ServerHolder> segmentCountMap = new TreeMap<Number, ServerHolder>(Collections.reverseOrder());
		for (ServerHolder serverHolder : serverHolderList) {
      if (serverHolder.getServer().getSegment(segment.getIdentifier()) != null) {
        segmentCountMap.put(serverHolder.getServer().getSegments().size(), serverHolder);
      }
		}

    int numReplicants = numReplicantsToRemove;
    for (Map.Entry<Number, ServerHolder> entry : segmentCountMap.entrySet()) {
      //log.info("Removing Segment from [%s] because it has [%d] segments", entry.getValue().getServer().getMetadata().toString(), entry.getKey().intValue());
      final ServerHolder holder = entry.getValue();
      stats.addToTieredStat(droppedCount, tier, 0);

      if (holder.isServingSegment(segment)) {
        replicationManager.registerReplicantTermination(
            tier,
            segment.getIdentifier(),
            holder.getServer().getHost()
        );
      }

      holder.getPeon().dropSegment(
          segment,
          new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
              replicationManager.unregisterReplicantTermination(
                  tier,
                  segment.getIdentifier(),
                  holder.getServer().getHost()
              );
            }
          }
      );

      --numReplicants;
      stats.addToTieredStat(droppedCount, tier, 1);
      log.info("Removed Segment [%s]", segment.getIdentifier());

			if (numReplicants == 0) {
        break;
			}
    }
    return stats;
  }
}
