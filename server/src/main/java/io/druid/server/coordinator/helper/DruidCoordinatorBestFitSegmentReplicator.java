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

import io.druid.client.ImmutableDruidServer;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.timeline.DataSegment;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;

public class DruidCoordinatorBestFitSegmentReplicator implements DruidCoordinatorHelper
{
	private final DruidCoordinator coordinator;

	private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorBestFitSegmentReplicator.class);
	private static final String assignedCount = "assignedCount";
	private static final String droppedCount = "droppedCount";
	private final ObjectMapper jsonMapper = new DefaultObjectMapper();
	private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);
	private final HttpClient httpClient;
	private final ServerDiscoveryFactory serverDiscoveryFactory;

	private static final long MIN_THRESHOLD = 5;

	public DruidCoordinatorBestFitSegmentReplicator(
			DruidCoordinator coordinator)
	{
		this.coordinator = coordinator;
		this.httpClient = coordinator.httpClient;
		this.serverDiscoveryFactory = coordinator.serverDiscoveryFactory;
	}

	@Override
	public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
	{
		//log.info("Starting replication. Getting Segment Popularity");
		final CoordinatorStats stats = new CoordinatorStats();
		HashMap<DataSegment, Long> insertList = new HashMap<DataSegment, Long>();
		HashMap<DataSegment, Long> removeList = new HashMap<DataSegment, Long>();

		// Acquire Query Workload in the last window
		Multiset<DataSegment> segments = HashMultiset.create();
		calculateSegmentCounts(segments);

		// Calculate the popularity map
		HashMap<DataSegment, Long> weightedAccessCounts = coordinator.getWeightedAccessCounts();
		calculateWeightedAccessCounts(params, segments, weightedAccessCounts, removeList);
		coordinator.setWeightedAccessCounts(weightedAccessCounts);

		// Calculate replication based on popularity
		//calculateAdaptiveReplication(params, weightedAccessCounts, insertList, removeList);
		HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable = new HashMap<DataSegment, HashMap<DruidServerMetadata, Long>>();
		calculateBestFitReplication(params, weightedAccessCounts, routingTable, insertList, removeList);

		// Load new segments
		loadNewSegments(params, stats, routingTable);

		coordinator.setRoutingTable(routingTable);

		// Manage replicas
		//manageReplicas(params, insertList, removeList, stats);
		manageReplicas(params, routingTable, stats);

		return params.buildFromExisting() 
				.withCoordinatorStats(stats) 
				.build();
	}
	
	private List<String> getBrokerURLs()
	{
		String brokerservice = TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME;
		ServerDiscoverySelector selector = serverDiscoveryFactory.createSelector(brokerservice);
		try {
			selector.start();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Collection<Server> brokers = selector.getAll();
		
		List<String> uris = new ArrayList<String>(brokers.size());
		for (Server broker : brokers)
		{
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
						null);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.info("URI [%s]", uri.toString());
			
			uris.add(uri.toString());
		}

		log.info("Number of Broker Servers [%d]", brokers.size());

		return uris;
  	}

	private void calculateSegmentCounts(Multiset<DataSegment> segmentCounts)
	{
		//log.info("Starting replication. Getting Segment Popularity");
		List<String> urls = getBrokerURLs();

		ExecutorService pool = Executors.newFixedThreadPool(urls.size());
		List<Future<List<DataSegment>>> futures = new ArrayList<Future<List<DataSegment>>>();
			
		for (final String url: urls)
		{
			futures.add(pool.submit(new Callable<List<DataSegment>> (){
				@Override
				public List<DataSegment> call()
				{
					List<DataSegment> segments = new ArrayList<DataSegment>();
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
				        
				        segments = jsonMapper.readValue(
				            response.getContent(), new TypeReference<List<DataSegment>>()
				            {
				            }
				        );
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
		
		List<DataSegment> segments = new ArrayList<DataSegment>();
		for (Future<List<DataSegment>> future: futures)
		{
			try {
				segments = future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for (DataSegment segment : segments)
			{
				if (segment != null)
					segmentCounts.add(segment);
			}
		}
		
		/*for (Entry<DataSegment> entry : segmentCounts.entrySet())
		{
			if (entry != null)
				log.info("Segment Received [%s] Count [%d]", entry.getElement().getIdentifier(), entry.getCount());
		}*/
	}

	private void calculateWeightedAccessCounts(DruidCoordinatorRuntimeParams params, Multiset<DataSegment> segments, HashMap<DataSegment, Long> weightedAccessCounts, HashMap<DataSegment, Long> removeList)
	{
		//log.info("Calculating Weighted Access Counts for Segments");

		// Handle those segments which are in Coordinator's map but not in segments collected from query
		for (Map.Entry<DataSegment, Long> entry : weightedAccessCounts.entrySet())
			if (segments.contains(entry.getKey()) == false)
				weightedAccessCounts.put(entry.getKey(), (long)Math.ceil(0.5 * entry.getValue()));

		for (Entry<DataSegment> entry : segments.entrySet())
		{
			DataSegment segment = entry.getElement();
			int segmentCount = entry.getCount();

			if (weightedAccessCounts.containsKey(segment) == false)
				weightedAccessCounts.put(segment, (long)segmentCount);
			else
			{
				double popularity = weightedAccessCounts.get(segment).doubleValue();
				weightedAccessCounts.put(segment, (long)Math.ceil(segmentCount + 0.5 * popularity));
			}
		}
		
		// Remove segments with counts less than a threshold from weightedAccessCounts. Also add it to removeList
        Iterator it = weightedAccessCounts.entrySet().iterator();
		while (it.hasNext())
		{
            Map.Entry<DataSegment, Long> entry = (Map.Entry<DataSegment, Long>)it.next();
			//log.info("Segment Received [%s] Count [%f]", entry.getKey().getIdentifier(), entry.getValue().doubleValue());

			if (entry.getValue() < MIN_THRESHOLD)
			{
				DataSegment segment = entry.getKey();
				removeList.put(segment, (long)params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier()));
				it.remove();
			}
		}
	}

	private void calculateAdaptiveReplication(DruidCoordinatorRuntimeParams params, HashMap<DataSegment, Long> weightedAccessCounts, HashMap<DataSegment, Long> insertList, HashMap<DataSegment, Long> removeList)
	{
		//log.info("Calculating Replication for Segments");
		int historicalNodeCount = 0;
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
			historicalNodeCount += serverQueue.size(); 

		long totalWeightCount = 0;
		for (long number : weightedAccessCounts.values())
			totalWeightCount += number;
		
		long all = -1;

		for (Map.Entry<DataSegment, Long> entry : weightedAccessCounts.entrySet())
		{
			DataSegment segment = entry.getKey();
			int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());

			long newReplicationFactor = (long)Math.ceil(entry.getValue().doubleValue() * historicalNodeCount / totalWeightCount);

			if (newReplicationFactor == 0)
				removeList.put(segment, all);
			else if (newReplicationFactor < totalReplicantsInCluster)
				removeList.put(segment, totalReplicantsInCluster - newReplicationFactor);
			else if (newReplicationFactor > totalReplicantsInCluster)
				insertList.put(segment, newReplicationFactor - totalReplicantsInCluster);
		}
	}

	private void calculateBestFitReplication(
			DruidCoordinatorRuntimeParams params,
			HashMap<DataSegment, Long> weightedAccessCounts,
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable,
			HashMap<DataSegment, Long> insertList,
			HashMap<DataSegment, Long> removeList)
	{
		log.info("Calculating Best Fit Replication for Segments");
		int historicalNodeCount = 0;
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
			historicalNodeCount += serverQueue.size(); 

		double totalWeightCount = 0;
		for (long number : weightedAccessCounts.values())
			totalWeightCount += number;

		log.info("Total Weight Count [%f]", totalWeightCount);
		
		long slotsperhn = (long) Math.ceil((double)totalWeightCount / historicalNodeCount);

		HashMap<DruidServerMetadata, Long> nodeCapacities = new HashMap<DruidServerMetadata, Long>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
		{
			for (ServerHolder holder : serverQueue)
			{
				nodeCapacities.put(holder.getServer().getMetadata(), slotsperhn);
			}
		}

		PriorityQueue<Tuple> maxheap = new PriorityQueue<Tuple>(10, new Comparator<Tuple>()
		{
			public int compare(Tuple o1, Tuple o2){
				int result = -Long.compare(o1.weight, o2.weight);
				return result;
			}
		});
		
		for(Map.Entry<DataSegment, Long> entry : weightedAccessCounts.entrySet())
		{
			maxheap.add(new Tuple(entry.getKey(), entry.getValue()));
		}
		
		Multiset<DataSegment> expectedCount = HashMultiset.create();
		while (maxheap.peek() != null)
		{
			Tuple candidate = maxheap.poll();
			//log.info("Chosen Segment [%s] [%f]", candidate.segment.getIdentifier(), candidate.weight);
			
			long valleft = bestFit(candidate, nodeCapacities, routingTable);
			expectedCount.add(candidate.segment);
			if (valleft > 0)
			{
				maxheap.add(new Tuple(candidate.segment, valleft));
			}
		}

		/*for (Entry<DataSegment> entry : expectedCount.entrySet())
		{
			long totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(entry.getElement().getIdentifier());
			long newReplicationFactor = entry.getCount();
			
			log.info("Replication Decision for [%s]: Current [%d] Required [%d]", entry.getElement().getIdentifier(), totalReplicantsInCluster, newReplicationFactor);

			if (newReplicationFactor < totalReplicantsInCluster)
				removeList.put(entry.getElement(), totalReplicantsInCluster - newReplicationFactor);
			else if (newReplicationFactor > totalReplicantsInCluster)
				insertList.put(entry.getElement(), newReplicationFactor - totalReplicantsInCluster);
		}*/
		
        /*for (Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet())
			for (DruidServerMetadata server : entry.getValue().keySet())
                log.info("Server [%s] should have segment [%s]", server.getHost(), entry.getKey().getIdentifier());*/
	}

	private long bestFit(
			Tuple candidate,
			HashMap<DruidServerMetadata, Long> nodeCapacities,
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable)
	{
        //log.info("Segment [%s] Weight [%d]", candidate.segment.getIdentifier(), candidate.weight);
		long val = candidate.weight;
		if (!routingTable.containsKey(candidate.segment))
			routingTable.put(candidate.segment, new HashMap<DruidServerMetadata, Long>());

		long mincapleftafterfill = Integer.MAX_VALUE;
		long minvalleftafterfill = Integer.MAX_VALUE;
		DruidServerMetadata minfitServer = null;
		DruidServerMetadata minspillServer = null;
		boolean fits = false;
		long empty = 0;
		for (Map.Entry<DruidServerMetadata, Long> entry : nodeCapacities.entrySet())
		{
            //log.info("Server [%s] Capacity [%d]", entry.getKey().getHost(), entry.getValue());
			long capacity = entry.getValue();
            if (capacity == 0)
                continue;

			if(val <= capacity)
			{
				fits = true;
				long leftafterfill = capacity - val;
				if (leftafterfill < mincapleftafterfill)
				{
					mincapleftafterfill = leftafterfill;
					minfitServer = entry.getKey();
                    //log.info("Server [%s] fits segment [%s]", minfitServer.getHost(), candidate.segment.getIdentifier());
				}
			}
			else
			{
				long leftafterfill = val - capacity;
				if(leftafterfill < minvalleftafterfill)
				{
					minvalleftafterfill = leftafterfill;
					minspillServer = entry.getKey();
                    //log.info("Server [%s] spills segment [%s]", minspillServer.getHost(), candidate.segment.getIdentifier());
				}
			}
		}
		
		if (fits == true)
		{
			routingTable.get(candidate.segment).put(minfitServer, val);
			nodeCapacities.put(minfitServer, mincapleftafterfill);
			return 0;
		}
		else
		{
			routingTable.get(candidate.segment).put(minspillServer, val - minvalleftafterfill);
			nodeCapacities.put(minspillServer, empty);
			return minvalleftafterfill;
		}
	}
	
	private void manageReplicas(DruidCoordinatorRuntimeParams params, HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable, CoordinatorStats stats)
	{
		//log.info("Managing Replicas by inserting and removing replicas for relevant data segments");

		HashMap<ImmutableDruidServer, ServerHolder> serverHolderMap = new HashMap<ImmutableDruidServer, ServerHolder>();
		HashMap<DruidServerMetadata, ServerHolder> serverMetaDataMap = new HashMap<DruidServerMetadata, ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()){
            for (ServerHolder holder : serverQueue){
			    serverHolderMap.put(holder.getServer(), holder);
			    serverMetaDataMap.put(holder.getServer().getMetadata(), holder);
            }
		}
		
		if (serverHolderMap.size() == 0) {
			log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
			return;
		}
		
		HashMap<DataSegment, List<DruidServerMetadata>> currentTable = new HashMap<DataSegment, List<DruidServerMetadata>>();		
		for (ImmutableDruidServer server : serverHolderMap.keySet())
		{
			for (DataSegment segment : server.getSegments().values())
			{
                log.info("Server [%s] has segment [%s]", server.getHost(), segment.getIdentifier());
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
		
		for (Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet())
		{
			DataSegment segment = entry.getKey();
			for (DruidServerMetadata server : entry.getValue().keySet())
			{
                //log.info("Server [%s] should have segment [%s]", server.getHost(), segment.getIdentifier());
				if (!currentTable.containsKey(segment) || !currentTable.get(segment).contains(server))
				{
					CoordinatorStats assignStats = assign(
							params.getReplicationManager(),
							tier,
							serverMetaDataMap.get(server),
							segment
							);
					stats.accumulate(assignStats);
				}
			}
		}
		
		for (Map.Entry<DataSegment, List<DruidServerMetadata>> entry : currentTable.entrySet())
		{
			DataSegment segment = entry.getKey();
			if(entry.getValue().size()>1){
				int count = entry.getValue().size();
				for (DruidServerMetadata server : entry.getValue())
				{
					if ((count>1) && (!routingTable.containsKey(segment) || !routingTable.get(segment).keySet().contains(server)))
					{
						log.info("[GETAFIX PLACEMENT] Dropping " + segment.getIdentifier() + " from " + server.getHost());
						CoordinatorStats dropStats = drop(
								params.getReplicationManager(),
								tier,
								serverMetaDataMap.get(server),
								segment
								);
						stats.accumulate(dropStats); 
						count--;
					}
				}
			}
			
		}
	}

	private void loadNewSegments(
			DruidCoordinatorRuntimeParams params,
			CoordinatorStats stats,
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable
	) {
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

		Set<DataSegment> orderedAvailableDataSegments = coordinator.getOrderedAvailableDataSegments();
		log.info("[GETAFIX PLACEMENT] latest segment: " + coordinator.getLatestSegment());

		for (DataSegment segment : orderedAvailableDataSegments) {
			if (segment.getIdentifier().equals(coordinator.getLatestSegment())) {
				break;
			}

			CoordinatorStats assignStats = assign(
					params.getReplicationManager(),
					tier,
					strategy,
					serverHolderList,
					segment,
					1L,
					routingTable
			);
			stats.accumulate(assignStats);
		}

		if (!orderedAvailableDataSegments.isEmpty()) {
			coordinator.setLatestSegment(orderedAvailableDataSegments.iterator().next().getIdentifier());
		}
	}

	private CoordinatorStats assign(
			final ReplicationThrottler replicationManager,
			final String tier,
			final ServerHolder holder,
			final DataSegment segment
			)
	{
		log.info("Insert Segment [%s] to [%s]", segment.getIdentifier(), holder.getServer().getHost());
		
        final CoordinatorStats stats = new CoordinatorStats();
		stats.addToTieredStat(assignedCount, tier, 0);

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

		return stats;
	}
	
	private CoordinatorStats drop(
			final ReplicationThrottler replicationManager,
			final String tier,
			final ServerHolder holder,
			final DataSegment segment
			)
	{
		log.info("Remove Segment [%s] from [%s]", segment.getIdentifier(), holder.getServer().getHost());
		CoordinatorStats stats = new CoordinatorStats();

		//log.info("Removing Segment from [%s] because it has [%d] segments", entry.getValue().getServer().getMetadata().toString(), entry.getKey().intValue());
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
			
		stats.addToTieredStat(droppedCount, tier, 1);
		log.info("Removed Segment [%s]", segment.getIdentifier());
			
		return stats;
	}

	private void manageReplicas(DruidCoordinatorRuntimeParams params, HashMap<DataSegment, Long> insertList, HashMap<DataSegment, Long> removeList, CoordinatorStats stats)
	{
		//log.info("Managing Replicas by inserting and removing replicas for relevant data segments");

		List<ServerHolder> serverHolderList = new ArrayList<ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
			serverHolderList.addAll(serverQueue);

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

		for (Map.Entry<DataSegment, Long> entry : insertList.entrySet())
		{
			String id = entry.getKey().getIdentifier();
            DataSegment segment = null;
            for (DataSegment candidate : coordinator.getAvailableDataSegments())
            {
		        //log.info("Comparing candidate: %s to incumbent %s", candidate.getIdetifier(), id);
                if (candidate.getIdentifier().equals(id))
                {
                    segment = candidate;
                    break;
                }
            }

            if (segment == null)
                continue;

			CoordinatorStats assignStats = assign(
					params.getReplicationManager(),
					tier,
					strategy,
					serverHolderList,
					segment,
					entry.getValue(),
					null
					);
			stats.accumulate(assignStats);
		}

		for (Map.Entry<DataSegment, Long> entry : removeList.entrySet())
		{
			String id = entry.getKey().getIdentifier();
            DataSegment segment = null;
            for (DataSegment candidate : coordinator.getAvailableDataSegments())
            {
                if (candidate.getIdentifier().equals(id))
                {
                    segment = candidate;
                    break;
                }
            }

            if (segment == null)
                continue;

			int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());
			if (totalReplicantsInCluster <= 0) {
				continue;
			}

			long numReplicantsToRemove = entry.getValue() == -1 ? totalReplicantsInCluster : entry.getValue();
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
			final long numReplicantsToAdd,
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable
	)
	{
		log.info("Insert Segment [%s] [%d]", segment.getIdentifier(), numReplicantsToAdd);
		
        final CoordinatorStats stats = new CoordinatorStats();
		stats.addToTieredStat(assignedCount, tier, 0);

		long numReplicants = numReplicantsToAdd;

		HashMap<DruidServerMetadata, Long> bootstrapRouting = new HashMap<>();
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
			bootstrapRouting.put(holder.getServer().getMetadata(), 1L);

			stats.addToTieredStat(assignedCount, tier, 1);
			--numReplicants;
		}

		routingTable.put(segment, bootstrapRouting);

		return stats;
	}

	private CoordinatorStats drop(
			final ReplicationThrottler replicationManager,
			final String tier,
			final List<ServerHolder> serverHolderList,
			final DataSegment segment,
			final long numReplicantsToRemove
			)
	{
		log.info("Remove Segment [%s] [%d]", segment.getIdentifier(), numReplicantsToRemove);
		CoordinatorStats stats = new CoordinatorStats();

		// Pick the server which has the maximum number of segments for load balance
		Map<Number, ServerHolder> segmentCountMap = new TreeMap<Number, ServerHolder>(Collections.reverseOrder());
		for (ServerHolder serverHolder : serverHolderList)
			if (serverHolder.getServer().getSegment(segment.getIdentifier()) != null)
				segmentCountMap.put(serverHolder.getServer().getSegments().size(), serverHolder);

		long numReplicants = numReplicantsToRemove;
		for (Map.Entry<Number, ServerHolder> entry : segmentCountMap.entrySet())
		{
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
			
            if (numReplicants == 0)
				break;
		}
		return stats;
	}
	
	
	private int[] hungarianMethod(int[][] costMatrix){
		int[] ret = new int[costMatrix.length];
		if(costMatrix.length==0){
			return ret;
		}
		Arrays.fill(ret, -1);
		//deduct smallest from each row and coloumn
		int size = costMatrix.length;
		int[] rowMin = new int[size];
		int[] colMin = new int[size];
		Arrays.fill(rowMin, Integer.MAX_VALUE);
		Arrays.fill(colMin, Integer.MAX_VALUE);
		for(int i=0; i<size;i++){
			for(int j=0;j<size;j++){
				if(costMatrix[i][j]<rowMin[i]){
					rowMin[i] = costMatrix[i][j];
				}
				if(costMatrix[i][j]<rowMin[j]){
					colMin[j] = costMatrix[i][j];
				}
			}
		}
		while(true){
			//deduct and find the uncovered minimum
			HashSet<Integer> coveredRow = new HashSet<Integer>();
			HashSet<Integer> coveredCol = new HashSet<Integer>();
			boolean[] markedRows = new boolean[size];
			boolean[] markedCols = new boolean[size];
			
			for(int i = 0;i<size;i++){
				for(int j = 0; j<size; j++){
					costMatrix[i][j] = costMatrix[i][j] - rowMin[i];
					costMatrix[i][j] = costMatrix[i][j] - colMin[j];
					if(costMatrix[i][j]==0){
						if(!coveredCol.contains(j) && !coveredRow.contains(i)){
							coveredCol.add(j);
							coveredRow.add(i);
							ret[i]=j;
						}
						
					}
				}
			}
			
			if(coveredCol.size()>=size){
				return ret;
			}
			
			//mark the matrix
			for(int i=0;i<size;i++){
				if(!coveredRow.contains(i)){
					//no assignment for this row
					markedRows[i]=true;
					for(int j=0;j<size;j++){
						if(costMatrix[i][j]==0){
							//mark all column on this row that has 0
							markedCols[j]=true;
							for(int k = 0;k<size;k++){
								if(costMatrix[k][j]==0){
									markedRows[k]=true;
								}
							}
						}
					}
				}
			}
			
			
			//find unmarked minimum
			int unmarkedMin = Integer.MAX_VALUE;
			for(int i = 0; i<size; i++){
				for(int j = 0;j<size;j++){
					if(markedRows[i]==false && markedCols[j]==true){
						//unmarked rows and marked column
						if(costMatrix[i][j]<unmarkedMin){
							unmarkedMin = costMatrix[i][j];
						}
					}
				}
			}
			//deduct min from unmarked entry and add to marked entry
			for(int i = 0; i<size; i++){
				for(int j = 0;j<size;j++){
					if(markedRows[i]==false && markedCols[j]==true){
						costMatrix[i][j] -= unmarkedMin;
					}
					else{
						costMatrix[i][j] += unmarkedMin;
					}
				}
			}
		}
		
		//PriorityQueue<Integer> minQueue = new PriorityQueue<Integer>();

	}
}
