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
import com.google.api.client.util.Data;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;

import io.druid.client.ImmutableDruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DruidCoordinatorBestFitSegmentReplicator implements DruidCoordinatorHelper {
	private final DruidCoordinator coordinator;

	private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorBestFitSegmentReplicator.class);
	private final ObjectMapper jsonMapper = new DefaultObjectMapper();
	private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);
	private final HttpClient httpClient;
	private final ReplicationThrottler replicatorThrottler;
	private static final long MIN_THRESHOLD = 5;
	private static int bestFitReplicationRound = 0;
	private Map<DruidServerMetadata, Long> hnNumQueriesProcessed;
	private Map<DruidServerMetadata, Double> hnQueryLoadWeight;
	private long totalQueryProcessedByAllHns;
	private Map<DruidServerMetadata, Long> hnQueryTimeProcessed;
	private Map<DruidServerMetadata, Double> hnQueryTimeWeight;
	private long totalQueryTimeProcessedByAllHns;
	private long totalQueriesSinceDeployment;
	private boolean hetNodeTrainingComplete;

	public DruidCoordinatorBestFitSegmentReplicator(
					DruidCoordinator coordinator) {
		this.coordinator = coordinator;
		this.httpClient = coordinator.httpClient;
		this.replicatorThrottler = new ReplicationThrottler(
						coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
						coordinator.getDynamicConfigs().getReplicantLifetime()
		);
		this.hnNumQueriesProcessed = new HashMap<DruidServerMetadata, Long>();
		this.hnQueryLoadWeight = new HashMap<DruidServerMetadata, Double>();
		this.totalQueryProcessedByAllHns = 0;
		this.hnQueryTimeProcessed = new HashMap<DruidServerMetadata, Long>();
		this.hnQueryTimeWeight = new HashMap<DruidServerMetadata, Double>();
		this.totalQueryTimeProcessedByAllHns = 0;
		this.totalQueriesSinceDeployment = 0;
		this.hetNodeTrainingComplete = false;
	}

	@Override
	public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params) {
		//log.debug("Starting replication. Getting Segment Popularity");
		replicatorThrottler.updateParams(
						coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
						coordinator.getDynamicConfigs().getReplicantLifetime()
		);

		final CoordinatorStats stats = new CoordinatorStats();

		// print cluster configuration
		//printClusterConfig(params);

		// Acquire Query Workload in the last window
		Multiset<DataSegment> segments = HashMultiset.create();
		calculateSegmentCounts(params, segments);

		// Calculate the popularity map
		HashMap<DataSegment, Long> weightedAccessCounts = coordinator.getWeightedAccessCounts();
		calculateWeightedAccessCounts(params, segments, weightedAccessCounts);
		coordinator.setWeightedAccessCounts(weightedAccessCounts);

		// calculate hn weights based on num queries. This accounts for heterogeneity in the cluster
		//calculateHnQueryWeights();

		// calculate hn weights based on query time. This accounts for heterogeneity in the cluster
		//calculateHnQueryTimeWeights();

		// Calculate replication based on popularity
		HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable = new HashMap<DataSegment, HashMap<DruidServerMetadata, Long>>();
		calculateBestFitReplication(params, weightedAccessCounts, routingTable);

		// Load new segments
		DruidCoordinatorReplicatorHelper.loadNewSegments(params, routingTable, coordinator);

		coordinator.setRoutingTable(routingTable);

		// Manage replicas
		manageReplicas(params, routingTable, stats);

		return params.buildFromExisting()
						.withReplicationManager(replicatorThrottler)
						.withCoordinatorStats(stats)
						.build();
	}

	private List<URI> getHistoricalURLs(DruidCoordinatorRuntimeParams params) {
		List<ImmutableDruidServer> historicals = new ArrayList<ImmutableDruidServer>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
			for (ServerHolder holder : serverQueue) {
				historicals.add(holder.getServer());
			}
		}

		List<URI> uris = new ArrayList<URI>(historicals.size());
		for (ImmutableDruidServer historical : historicals) {
			// Should use threads to fetch in parallel from all brokers
			URI uri = null;
			try {
				uri = new URI(
								"http",
								null,
								historical.getHost().split(":")[0],
								Integer.parseInt(historical.getHost().split(":")[1]),
								"/druid/historical/v1/totalAccessTime",
								null,
								null);
			} catch (URISyntaxException e) {
				log.warn("URI did not get created [%s]", historical.getHost());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			//log.debug("URI [%s]", uri.toString());

			uris.add(uri);
		}

		//log.debug("Number of Historical Servers [%d]", historicals.size());

		return uris;
	}

	private List<DruidServerMetadata> getDruidServerList(DruidCoordinatorRuntimeParams params) {
		List<DruidServerMetadata> servers = new ArrayList<DruidServerMetadata>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
			for (ServerHolder holder : serverQueue) {
				servers.add(holder.getServer().getMetadata());
			}
		}
		return servers;
	}

	private void calculateSegmentCounts(DruidCoordinatorRuntimeParams params, Multiset<DataSegment> segmentCounts) {

		List<DruidServerMetadata> servers = getDruidServerList(params);
		List<URI> urls = getHistoricalURLs(params);

		ExecutorService pool = Executors.newFixedThreadPool(urls.size());
		List<Future<Map<String, Integer>>> futures = new ArrayList<Future<Map<String, Integer>>>();

		for (final URI url : urls) {
			futures.add(pool.submit(new Callable<Map<String, Integer>>() {
				@Override
				public Map<String, Integer> call() {
					Map<String, Integer> totalAccessMap = Maps.newHashMap();
					try {
						StatusResponseHolder response = httpClient.go(
										new Request(
														HttpMethod.GET,
														new URL(url.toString())
										),
										responseHandler
						).get();

						if (!response.getStatus().equals(HttpResponseStatus.OK)) {
							throw new ISE(
											"Error while querying[%s] status[%s] content[%s]",
											url.toString(),
											response.getStatus(),
											response.getContent()
							);
						}

						//log.debug("Response Length [%d]", response.getContent().length());

						totalAccessMap = jsonMapper.readValue(
										response.getContent(), new TypeReference<Map<String, Integer>>() {
										}
						);
					} catch (Exception e) {
						e.printStackTrace();
						throw Throwables.propagate(e);
					}

					for (Map.Entry<String, Integer> entry : totalAccessMap.entrySet()) {
						log.debug("Segment Received [%s] with value [%d] from [%s]", entry.getKey(), entry.getValue(), url.getHost());
					}

					return totalAccessMap;
				}
			}));
		}



		HashMap<ImmutableDruidServer, ServerHolder> serverHolderMap = new HashMap<ImmutableDruidServer, ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
			for (ServerHolder holder : serverQueue) {
				serverHolderMap.put(holder.getServer(), holder);
			}
		}

		if (serverHolderMap.size() == 0) {
			log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
			return;
		}

		Map<String, DataSegment> datasegments = Maps.newHashMap();
		for (ImmutableDruidServer server : serverHolderMap.keySet()) {
			for (DataSegment segment : server.getSegments().values()) {
				datasegments.put(segment.getIdentifier(), segment);
			}
		}

		int index = 0;
		hnNumQueriesProcessed.clear();
		totalQueryProcessedByAllHns = 0;
		hnQueryTimeProcessed.clear();
		totalQueryTimeProcessedByAllHns = 0;

		Map<String, Integer> segments = Maps.newHashMap();
		for (Future<Map<String, Integer>> future : futures) {
			try {
				segments = future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			DruidServerMetadata server = servers.get(index++);

			// remove the numQueriesProcessed entry from the segments map
			long numQueriesProcessed = 0;
//			numQueriesProcessed = segments.remove("numQueriesProcessed") + 1; // add 1 to avoid div by zero errors
//			hnNumQueriesProcessed.put(server, numQueriesProcessed);
//			totalQueryProcessedByAllHns += numQueriesProcessed;

			long totalHnQueryTime = 1; // add 1 to avoid div by zero errors
			for (Map.Entry<String, Integer> entry : segments.entrySet()) {
				DataSegment segment = datasegments.get(entry.getKey());
				if(segment == null){
					log.info("Received null segment from %s", server.getHost());
					continue;
				}
				if (segmentCounts.contains(segment)) {
					int currentCount = segmentCounts.count(segment);
					segmentCounts.setCount(segment, currentCount + entry.getValue());
				} else {
					segmentCounts.add(segment, entry.getValue());
				}
				totalHnQueryTime += entry.getValue();
			}

			hnQueryTimeProcessed.put(server, totalHnQueryTime);
			totalQueryTimeProcessedByAllHns += totalHnQueryTime;

			log.info("HN %s processed %d queries, total %d, querytime %d, total querytime %d ", server.getHost(),
							numQueriesProcessed, totalQueryProcessedByAllHns, totalHnQueryTime, totalQueryTimeProcessedByAllHns);
		}
		totalQueriesSinceDeployment += totalQueryProcessedByAllHns;
		//if(totalQueriesSinceDeployment > 15000){
			hetNodeTrainingComplete = true;
		//}

		for (Entry<DataSegment> entry : segmentCounts.entrySet()) {
			if (entry != null)
				log.debug("Segment Received [%s] Count [%d]", entry.getElement().getIdentifier(), entry.getCount());
		}
	}

	private void calculateHnQueryWeights(){

		//log.debug("Calculating HN query weights numHns %d ", hnNumQueriesProcessed.size());

		double alpha = 0.5;

		for(Map.Entry<DruidServerMetadata, Long> e : hnNumQueriesProcessed.entrySet()){
			if(!hnQueryLoadWeight.containsKey(e.getKey())){
				hnQueryLoadWeight.put(e.getKey(), (double)e.getValue()/(double)totalQueryProcessedByAllHns);
			}
			else{
				double perfBasedWeight = alpha*e.getValue()/(double)totalQueryProcessedByAllHns +
								(double)(1-alpha)*(double)hnQueryLoadWeight.get(e.getKey());
				log.debug("Performance based load hn %s load %f curr %d old %f", e.getKey(), perfBasedWeight, e.getValue(), hnQueryLoadWeight.get(e.getKey()));
				hnQueryLoadWeight.put(e.getKey(), perfBasedWeight);
			}
		}
	}

	private void calculateHnQueryTimeWeights(){

		//log.info("Calculating HN query time weights numHns %d ", hnQueryTimeProcessed.size());

		double alpha = 0.5;

		for(Map.Entry<DruidServerMetadata, Long> e : hnQueryTimeProcessed.entrySet()){
			if(!hnQueryTimeWeight.containsKey(e.getKey())){
				hnQueryTimeWeight.put(e.getKey(), (double)e.getValue()/(double)totalQueryTimeProcessedByAllHns);
			}
			else{
				double perfBasedWeight = alpha*e.getValue()/(double)totalQueryTimeProcessedByAllHns +
								(double)(1-alpha)*(double)hnQueryTimeWeight.get(e.getKey());
				log.debug("Performance based load hn %s load %f curr %d old %f", e.getKey(), perfBasedWeight, e.getValue(), hnQueryTimeWeight.get(e.getKey()));
				hnQueryTimeWeight.put(e.getKey(), perfBasedWeight);
			}
		}

//		long total = 13;
//		double small = (double)1.0/(double)total;
//		double large = (double)2.0/(double)total;
//		for(Map.Entry<DruidServerMetadata, Long> e : hnQueryTimeProcessed.entrySet()){
//			if(e.getKey().getHost().equals("pc714.emulab.net:8081") || e.getKey().getHost().equals("pc733.emulab.net:8081") ||
//							e.getKey().getHost().equals("pc828.emulab.net:8081")) {
//				hnQueryTimeWeight.put(e.getKey(), large);
//			}
//			else{
//				hnQueryTimeWeight.put(e.getKey(), small);
//			}
//		}
	}

	private void calculateWeightedAccessCounts(
					DruidCoordinatorRuntimeParams params,
					Multiset<DataSegment> segments,
					HashMap<DataSegment, Long> weightedAccessCounts) {
		float alpha = 0.5F;
		//log.debug("Calculating Weighted Access Counts for Segments");

		// Handle those segments which are in Coordinator's map but not in segments collected from query
		if (alpha > 0) {
			for (Map.Entry<DataSegment, Long> entry : weightedAccessCounts.entrySet())
				if (segments.contains(entry.getKey()) == false)
					weightedAccessCounts.put(entry.getKey(), (long) Math.ceil(alpha * entry.getValue()));
		}

		for (Entry<DataSegment> entry : segments.entrySet()) {
			DataSegment segment = entry.getElement();
			int segmentCount = entry.getCount();

			if (weightedAccessCounts.containsKey(segment) == false)
				weightedAccessCounts.put(segment, (long) segmentCount);
			else {
				double popularity = weightedAccessCounts.get(segment).doubleValue();
				weightedAccessCounts.put(segment, (long) Math.ceil(segmentCount + alpha * popularity));
			}
		}

//		log.debug("Weighted access count map");
//		for (Map.Entry<DataSegment, Long> entry : weightedAccessCounts.entrySet()){
//			log.debug("Segment %s value %d", entry.getKey().getIdentifier(), entry.getValue());
//		}

		// Remove segments with counts less than a threshold from weightedAccessCounts. Also add it to removeList
//		Iterator it = weightedAccessCounts.entrySet().iterator();
//		while (it.hasNext())
//		{
//            Map.Entry<DataSegment, Long> entry = (Map.Entry<DataSegment, Long>)it.next();
//			//log.debug("Segment Received [%s] Count [%f]", entry.getKey().getIdentifier(), entry.getValue().doubleValue());
//
//			if (entry.getValue() < MIN_THRESHOLD)
//			{
//				it.remove();
//			}
//		}
	}

	private void calculateBestFitReplication(
					DruidCoordinatorRuntimeParams params,
					HashMap<DataSegment, Long> weightedAccessCounts,
					HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable) {
		log.debug("Calculating Best Fit Replication for Segments");
		int historicalNodeCount = 0;
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
			historicalNodeCount += serverQueue.size();

		double totalWeightCount = 0;
		for (long number : weightedAccessCounts.values())
			totalWeightCount += number;

		log.debug("Total Weight Count [%f]", totalWeightCount);

		//Can try with a higher value than average load
		long averageLoad = (long) Math.ceil((double) totalWeightCount / historicalNodeCount);

		HashMap<DruidServerMetadata, Long> nodeCapacities = new HashMap<DruidServerMetadata, Long>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
			for (ServerHolder holder : serverQueue) {
				/* average load */
				nodeCapacities.put(holder.getServer().getMetadata(), averageLoad);

//				if(hetNodeTrainingComplete){
//				  /* num queries based load */
//					//long queryPerfBasedLoad = (long)Math.ceil(totalWeightCount*hnQueryLoadWeight.get(holder.getServer().getMetadata()));
//					//nodeCapacities.put(holder.getServer().getMetadata(), queryPerfBasedLoad);
//
//					/* query time based load */
//					long querytimePerfBasedLoad = (long)Math.ceil(totalWeightCount*hnQueryTimeWeight.get(holder.getServer().getMetadata()));
//					nodeCapacities.put(holder.getServer().getMetadata(), querytimePerfBasedLoad);
//					log.info("Allocated %d to hn %s ", querytimePerfBasedLoad, holder.getServer().getMetadata().getHost());
//				}
//				else {
//					nodeCapacities.put(holder.getServer().getMetadata(), averageLoad);
//					//log.debug("Allocated %d to hn %s ", averageLoad, holder.getServer().getMetadata().getHost());
//				}
			}
		}

		int capacity = weightedAccessCounts.size();
		if (capacity == 0)
			capacity = 1;

		PriorityQueue<Tuple> maxheap = new PriorityQueue<Tuple>(capacity, new Comparator<Tuple>() {
			public int compare(Tuple o1, Tuple o2) {
				int result = -Long.compare(o1.weight, o2.weight);
				return result;
			}
		});

		for (Map.Entry<DataSegment, Long> entry : weightedAccessCounts.entrySet()) {
			maxheap.add(new Tuple(entry.getKey(), entry.getValue()));
		}

		//1.1 map all server metadata to IDs
		HashMap<DruidServerMetadata, Integer> metadataToIDMap = DruidCoordinatorReplicatorHelper.metadataToID(nodeCapacities);
		//1.2 map all ID to server metadata
		HashMap<Integer, DruidServerMetadata> IDToMetadataMap = DruidCoordinatorReplicatorHelper.IDToMetadata(metadataToIDMap);

		//2. construct before map
		log.debug("Construct Before Map");
		//TODO: This is not a good way of constructing the before map. We should cluster state instead of using routingTable.
		HashMap<Integer, HashMap<DataSegment, Integer>> beforeMap = DruidCoordinatorReplicatorHelper.constructReverseMap(coordinator.getRoutingTable(), metadataToIDMap);

		while (maxheap.peek() != null) {
			Tuple candidate = maxheap.poll();
			//log.debug("Chosen Segment [%s] [%f]", candidate.segment.getIdentifier(), candidate.weight);

			long valleft = bestFit(candidate, nodeCapacities, routingTable);
			if (valleft > 0) {
				maxheap.add(new Tuple(candidate.segment, valleft));
			}
		}

		//3. construct after map
		log.debug("Construct After Map");
		HashMap<Integer, HashMap<DataSegment, Integer>> afterMap = DruidCoordinatorReplicatorHelper.constructReverseMap(routingTable, metadataToIDMap);

		if (beforeMap.size() > 0 && afterMap.size() > 0 && beforeMap.size() == afterMap.size()) {
			//4. build costMatrix
			double[][] costMatrix = DruidCoordinatorReplicatorHelper.buildCostMatrix(beforeMap, afterMap);

			//5. Hungarian Matching
			int[] hungarianMap = DruidCoordinatorReplicatorHelper.hungarianMatching(costMatrix);
			log.debug("calculateBestFitReplication: Result Mapping:");
			for (int i = 0; i < hungarianMap.length; i++)
				log.debug("Original [%s] New [%s]", i, hungarianMap[i]);

			int start = bestFitReplicationRound % historicalNodeCount;
			int[] roundRobinMap = new int[historicalNodeCount];
			for (int i = 0; i < historicalNodeCount; i++) {
				roundRobinMap[i] = (start + i) % historicalNodeCount;
			}
			bestFitReplicationRound++;
			log.debug("Round robin map is %s", Arrays.toString(roundRobinMap));

			Comparator<Map.Entry<DruidServerMetadata, Long>> valueComparator = new Comparator<Map.Entry<DruidServerMetadata, Long>>() {
				@Override public int compare(Map.Entry<DruidServerMetadata, Long> e1, Map.Entry<DruidServerMetadata, Long> e2) {
					Long v1 = e1.getValue();
					Long v2 = e2.getValue();
					return v2.compareTo(v1);
				}
			};

			Set<Map.Entry<DruidServerMetadata, Long>> entries = hnQueryTimeProcessed.entrySet();
			List<Map.Entry<DruidServerMetadata, Long>> nodesSortedByQueryTime = new ArrayList<Map.Entry<DruidServerMetadata, Long>>(entries);
			Collections.sort(nodesSortedByQueryTime, valueComparator);

			int[] queryTimeBasedMap = new int[historicalNodeCount];
			for (int i = 0; i < historicalNodeCount; i++) {
				queryTimeBasedMap[i] = metadataToIDMap.get(nodesSortedByQueryTime.get(i).getKey());
			}

			//6. replace all modified variable based on map
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> matchedTable;
			matchedTable = DruidCoordinatorReplicatorHelper.rebuildRouting(routingTable, queryTimeBasedMap, metadataToIDMap, IDToMetadataMap);
//			routingTable.clear();
//			routingTable.putAll(matchedTable);

			//7. balance the segments to reduce memory spikes
			//log.debug("RTUB");
			//DruidCoordinatorReplicatorHelper.printRoutingTable(routingTable);
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> segmentBalancedTable;
			segmentBalancedTable = DruidCoordinatorReplicatorHelper.balanceRoutingTableSegments(routingTable);
			routingTable.clear();
			routingTable.putAll(segmentBalancedTable);
			//log.debug("RTAB");
			//DruidCoordinatorReplicatorHelper.printRoutingTable(routingTable);
		}
	}

	private long bestFit(
					Tuple candidate,
					HashMap<DruidServerMetadata, Long> nodeCapacities,
					HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable) {


		//log.debug("Segment [%s] Weight [%d]", candidate.segment.getIdentifier(), candidate.weight);
		long val = candidate.weight;
		if (!routingTable.containsKey(candidate.segment))
			routingTable.put(candidate.segment, new HashMap<DruidServerMetadata, Long>());

		long mincapleftafterfill = Integer.MAX_VALUE;
		long minvalleftafterfill = Integer.MAX_VALUE;
		DruidServerMetadata minfitServer = null;
		DruidServerMetadata minspillServer = null;
		boolean fits = false;
		long empty = 0;

		for (Map.Entry<DruidServerMetadata, Long> entry : nodeCapacities.entrySet()) {
			//log.info("Server [%s] Capacity [%d]", entry.getKey().getHost(), entry.getValue());
			long capacity = entry.getValue();
			if (capacity == 0)
				continue;

			if (val <= capacity) {
				fits = true;
				long leftafterfill = capacity - val;
				if (leftafterfill < mincapleftafterfill) {
					mincapleftafterfill = leftafterfill;
					minfitServer = entry.getKey();
					//log.info("Server [%s] fits segment [%s]", minfitServer.getHost(), candidate.segment.getIdentifier());
				}
			} else {
				long leftafterfill = val - capacity;
				if (leftafterfill < minvalleftafterfill) {
					minvalleftafterfill = leftafterfill;
					minspillServer = entry.getKey();
					//log.info("Server [%s] spills segment [%s]", minspillServer.getHost(), candidate.segment.getIdentifier());
				}
			}
		}

		if (fits == true) {
			routingTable.get(candidate.segment).put(minfitServer, val);
			nodeCapacities.put(minfitServer, mincapleftafterfill);
			return 0;
		} else {
			routingTable.get(candidate.segment).put(minspillServer, val - minvalleftafterfill);
			nodeCapacities.put(minspillServer, empty);
			return minvalleftafterfill;
		}
	}

	private void printClusterConfig(DruidCoordinatorRuntimeParams params) {

		HashMap<ImmutableDruidServer, ServerHolder> serverHolderMap = new HashMap<ImmutableDruidServer, ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
			for (ServerHolder holder : serverQueue) {
				serverHolderMap.put(holder.getServer(), holder);
			}
		}

		if (serverHolderMap.size() == 0) {
			log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
			return;
		}

		HashMap<DataSegment, List<DruidServerMetadata>> currentTable = new HashMap<DataSegment, List<DruidServerMetadata>>();
		log.debug("Cluster configuration");
		for (ImmutableDruidServer server : serverHolderMap.keySet()) {
			for (DataSegment segment : server.getSegments().values()) {
				log.debug("Server [%s] has segment [%s]", server.getHost(), segment.getIdentifier());
			}
		}
	}

	private void manageReplicas(
					DruidCoordinatorRuntimeParams params,
					HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable,
					CoordinatorStats stats) {
		//log.debug("Managing Replicas by inserting and removing replicas for relevant data segments");

		HashMap<ImmutableDruidServer, ServerHolder> serverHolderMap = new HashMap<ImmutableDruidServer, ServerHolder>();
		HashMap<DruidServerMetadata, ServerHolder> serverMetaDataMap = new HashMap<DruidServerMetadata, ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
			for (ServerHolder holder : serverQueue) {
				serverHolderMap.put(holder.getServer(), holder);
				serverMetaDataMap.put(holder.getServer().getMetadata(), holder);
			}
		}

		if (serverHolderMap.size() == 0) {
			log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
			return;
		}

		HashMap<DataSegment, List<DruidServerMetadata>> currentTable = new HashMap<DataSegment, List<DruidServerMetadata>>();
		for (ImmutableDruidServer server : serverHolderMap.keySet()) {
			for (DataSegment segment : server.getSegments().values()) {
				log.debug("Server [%s] has segment [%s]", server.getHost(), segment.getIdentifier());
				if (!currentTable.containsKey(segment))
					currentTable.put(segment, new ArrayList<DruidServerMetadata>());
				currentTable.get(segment).add(server.getMetadata());
			}
		}

		final List<String> tierNameList = Lists.newArrayList(params.getDruidCluster().getTierNames());
		if (tierNameList.size() == 0) {
			log.makeAlert("Cluster has multiple tiers! Check your cluster configuration!").emit();
			return;
		}
		final String tier = tierNameList.get(0);

		for (Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet()) {
			DataSegment segment = entry.getKey();
			for (DruidServerMetadata server : entry.getValue().keySet()) {
				//log.debug("Server [%s] should have segment [%s]", server.getHost(), segment.getIdentifier());
				if (!currentTable.containsKey(segment) || !currentTable.get(segment).contains(server)) {
					CoordinatorStats assignStats = DruidCoordinatorReplicatorHelper.assign(
									replicatorThrottler,
									tier,
									serverMetaDataMap.get(server),
									segment
					);
					stats.accumulate(assignStats);
				}
			}
		}

		for (Map.Entry<DataSegment, List<DruidServerMetadata>> entry : currentTable.entrySet()) {
			DataSegment segment = entry.getKey();
			int numReplicas = entry.getValue().size();

			for (DruidServerMetadata server : entry.getValue()) {
				if (numReplicas <= 1) {
					break;
				}
				if (!routingTable.containsKey(segment) || !routingTable.get(segment).keySet().contains(server)) {
					log.debug("[GETAFIX PLACEMENT] Dropping " + segment.getIdentifier() + " from " + server.getHost());
					CoordinatorStats dropStats = DruidCoordinatorReplicatorHelper.drop(
									replicatorThrottler,
									tier,
									serverMetaDataMap.get(server),
									segment
					);
					stats.accumulate(dropStats);
					numReplicas--;
					//break; // Removing only 1 replica at a time
				}
			}
		}

//		HashMap<DataSegment, DruidServerMetadata> addList = new HashMap<DataSegment, DruidServerMetadata>();
//		for (Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet())
//		{
//			DataSegment segment = entry.getKey();
//			for (DruidServerMetadata server : entry.getValue().keySet())
//			{
//				//log.debug("Server [%s] should have segment [%s]", server.getHost(), segment.getIdentifier());
//				if (!currentTable.containsKey(segment) || !currentTable.get(segment).contains(server))
//				{
//					addList.put(segment, server);
//				}
//			}
//		}
//
//		// add and remove segment one after another
//		for(Map.Entry<DataSegment, DruidServerMetadata> item : addList.entrySet()){
//			// add the segment to the server
//			CoordinatorStats assignStats = DruidCoordinatorReplicatorHelper.assign(
//							replicatorThrottler,
//							tier,
//							serverMetaDataMap.get(item.getValue()),
//							item.getKey()
//							);
//			stats.accumulate(assignStats);
//
//			// delete the same segment from other server, if needed
//			if(!currentTable.containsKey(item.getKey())){
//				continue;
//			}
//
//			DataSegment segment = item.getKey();
//			int numReplicas = currentTable.get(item.getKey()).size();
//
//			//for (DruidServerMetadata server : entry.getValue())
//			for (DruidServerMetadata server : currentTable.get(item.getKey()))
//			{
//				if(numReplicas <= 1){
//					break;
//				}
//				if (!routingTable.containsKey(segment) || !routingTable.get(segment).keySet().contains(server))
//				{
//					log.debug("[GETAFIX PLACEMENT] Dropping " + segment.getIdentifier() + " from " + server.getHost());
//					CoordinatorStats dropStats = DruidCoordinatorReplicatorHelper.drop(
//									replicatorThrottler,
//									tier,
//									serverMetaDataMap.get(server),
//									segment
//					);
//					stats.accumulate(dropStats);
//					numReplicas--;
//					//break; // Removing only 1 replica at a time
//				}
//			}
	}
//
//		// remove other segments
//		for (Map.Entry<DataSegment, List<DruidServerMetadata>> entry : currentTable.entrySet())
//		{
//			DataSegment segment = entry.getKey();
//			int numReplicas = entry.getValue().size();
//
//			for (DruidServerMetadata server : entry.getValue())
//			{
//				if(numReplicas <= 1){
//					break;
//				}
//				if (!routingTable.containsKey(segment) || !routingTable.get(segment).keySet().contains(server))
//				{
//					log.debug("[GETAFIX PLACEMENT] Dropping " + segment.getIdentifier() + " from " + server.getHost());
//					CoordinatorStats dropStats = DruidCoordinatorReplicatorHelper.drop(
//							replicatorThrottler,
//							tier,
//							serverMetaDataMap.get(server),
//							segment
//							);
//					stats.accumulate(dropStats);
//					numReplicas--;
//					//break; // Removing only 1 replica at a time
//				}
//			}
//		}
}
