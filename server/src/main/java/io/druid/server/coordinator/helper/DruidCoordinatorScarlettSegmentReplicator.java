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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DruidCoordinatorScarlettSegmentReplicator implements DruidCoordinatorHelper
{
	private final DruidCoordinator coordinator;

	private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorScarlettSegmentReplicator.class);
	private final ObjectMapper jsonMapper = new DefaultObjectMapper();
	private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);
	private final HttpClient httpClient;
	private HashMap<DataSegment, Long> CCAMap = new HashMap<DataSegment, Long>();
    private final ReplicationThrottler replicatorThrottler;

	private static final long MIN_THRESHOLD = 5;

	public DruidCoordinatorScarlettSegmentReplicator(
			DruidCoordinator coordinator)
	{
		this.coordinator = coordinator;
		this.httpClient = coordinator.httpClient;
        this.replicatorThrottler = new ReplicationThrottler(
                coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
                coordinator.getDynamicConfigs().getReplicantLifetime()
            );
	}

	@Override
	public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
	{
		log.info("Starting replication. Getting Segment Popularity");
        replicatorThrottler.updateParams(
                coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
                coordinator.getDynamicConfigs().getReplicantLifetime()
            );

        final CoordinatorStats stats = new CoordinatorStats();

		// Acquire Query Workload in the last window
		Map<DataSegment, Integer> popularSegments = Maps.newHashMap();
		getSegmentsWithLargestContention(params, popularSegments);

		// Calculate the popularity map
		HashMap<DataSegment, Long> weightedAccessCounts = coordinator.getWeightedAccessCounts();
		calculateWeightedAccessCounts(params, popularSegments, weightedAccessCounts);
		coordinator.setWeightedAccessCounts(weightedAccessCounts);

		// Calculate replication based on popularity
		HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable = coordinator.getRoutingTable();
		HashMap<DruidServerMetadata, Double> nodeVolumes = coordinator.getNodeVolumes();
		HashMap<DataSegment, List<DruidServerMetadata>> currentTable = new HashMap<DataSegment, List<DruidServerMetadata>>();
		calculateScarlettReplication(params, weightedAccessCounts, currentTable, routingTable, nodeVolumes);

		// Load new segments
		DruidCoordinatorReplicatorHelper.loadNewSegments(params, routingTable, coordinator);
		coordinator.setNodeVolumes(nodeVolumes);
		coordinator.setRoutingTable(routingTable);
		
		printNodeVolumeMap(nodeVolumes);
		printRoutingTable(routingTable);

		// Manage replicas
		manageReplicas(params, routingTable, stats, currentTable);

		return params.buildFromExisting() 
                .withReplicationManager(replicatorThrottler)
				.withCoordinatorStats(stats) 
				.build();
	}
	
	private void printRoutingTable(
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable) {
		// TODO Auto-generated method stub
		log.info("===== Routing Table =====");

		for(Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet()){
			String list = "";
			log.info("Segment: [%s]", entry.getKey().getIdentifier());
			HashMap<DruidServerMetadata, Long> entryValue = entry.getValue();
			for(Map.Entry<DruidServerMetadata, Long> entry2 : entryValue.entrySet()){
				list = list + entry2.getKey().getHost() + ';';
			}
			log.info("[%s]", list);
		}
	}

	private void printNodeVolumeMap(
			HashMap<DruidServerMetadata, Double> nodeVolumes) {
		// TODO Auto-generated method stub
		log.info("===== Node Volumes =====");
		for(Map.Entry<DruidServerMetadata, Double> entry : nodeVolumes.entrySet()){
			log.info("Server: [%s], Score [%s]", entry.getKey().getHost(), entry.getValue());
		}
	}

	private void printCurrentTable(HashMap<DataSegment, List<DruidServerMetadata>> currentTable) {
		log.info("===== CurrentTable =====");

		for(Map.Entry<DataSegment, List<DruidServerMetadata>> entry : currentTable.entrySet()){
			String list = "";
			log.info("Segment: [%s]", entry.getKey().getIdentifier());
			for(DruidServerMetadata m : entry.getValue()){
				list = list + m.getHost()+';';
			}
			log.info("[%s]", list);
		}
	}

	private List<String> getHistoricalURLs(DruidCoordinatorRuntimeParams params)
	{
		List<ImmutableDruidServer> historicals = new ArrayList<ImmutableDruidServer>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()){
            for (ServerHolder holder : serverQueue){
			    historicals.add(holder.getServer());
            }
		}
		
		List<String> uris = new ArrayList<String>(historicals.size());
		for (ImmutableDruidServer historical : historicals)
		{
			// Should use threads to fetch in parallel from all brokers
			URI uri = null;
			try {
				uri = new URI(
					    "http",
						null,
						historical.getHost().split(":")[0],
						Integer.parseInt(historical.getHost().split(":")[1]),
						"/druid/historical/v1/concurrentAccess",
						null,
						null);
			} catch (URISyntaxException e) {
                log.warn("URI did not get created [%s]", historical.getHost());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			log.info("URI [%s]", uri.toString());
			
			uris.add(uri.toString());
		}

		log.info("Number of Historical Servers [%d]", historicals.size());

		return uris;
  	}

	private void getSegmentsWithLargestContention(DruidCoordinatorRuntimeParams params, Map<DataSegment, Integer> contendedSegments)
	{
		List<String> urls = getHistoricalURLs(params);

		ExecutorService pool = Executors.newFixedThreadPool(urls.size());
		List<Future<Map<String, Integer>>> futures = new ArrayList<Future<Map<String, Integer>>>();
			
		for (final String url: urls)
		{
			futures.add(pool.submit(new Callable<Map<String, Integer>> (){
				@Override
				public Map<String, Integer> call()
				{
					Map<String, Integer> concurrentAccessMap = Maps.newHashMap();
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
				        
				        concurrentAccessMap = jsonMapper.readValue(
				            response.getContent(), new TypeReference<Map<String, Integer>>()
				            {
				            }
				        );
				      }
				      catch (Exception e) {
				    	e.printStackTrace();
				        throw Throwables.propagate(e);
				      }
				    
				    for (Map.Entry<String, Integer> entry : concurrentAccessMap.entrySet())
				    {
				        //log.info("Segment Received [%s] with value [%d]", entry.getKey(), entry.getValue());
				    }
				    
				    return concurrentAccessMap;
				}
			}));
		}
		
		HashMap<ImmutableDruidServer, ServerHolder> serverHolderMap = new HashMap<ImmutableDruidServer, ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()){
            for (ServerHolder holder : serverQueue){
			    serverHolderMap.put(holder.getServer(), holder);
            }
		}
		
		if (serverHolderMap.size() == 0) {
			log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
			return;
		}
		
		Map<String, DataSegment> datasegments = Maps.newHashMap();		
		for (ImmutableDruidServer server : serverHolderMap.keySet())
		{
			for (DataSegment segment : server.getSegments().values())
			{
				datasegments.put(segment.getIdentifier(), segment);
			}
		}
		
		Map<String, Integer> segments = Maps.newHashMap();
		for (Future<Map<String, Integer>> future: futures)
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
			
			for (Map.Entry<String, Integer> entry : segments.entrySet())
			{
				DataSegment segment = datasegments.get(entry.getKey());
				if (!contendedSegments.containsKey(segment))
					contendedSegments.put(segment, entry.getValue());
				else
					contendedSegments.put(segment, Math.max(contendedSegments.get(segment), entry.getValue()));
			}
		}
		
		for (Map.Entry<DataSegment, Integer> entry : contendedSegments.entrySet())
		{
			log.info("Segment Received [%s] Count [%d]", entry.getKey().getIdentifier(), entry.getValue());
		}
	}

	private void calculateWeightedAccessCounts(DruidCoordinatorRuntimeParams params, Map<DataSegment, Integer> popularSegments, HashMap<DataSegment, Long> weightedAccessCounts)
	{
		log.info("Calculating Weighted Access Counts for Segments");

		long delta = 1;
		int historicalNodeCount = 0;
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
			historicalNodeCount += serverQueue.size();
		
		if (historicalNodeCount == 0) {
			log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
			return;
		}
		Long numServers = Long.valueOf(historicalNodeCount);
		
		//clean up concurrency map
		//this.CCAMap = new HashMap<DataSegment, Long>();

		for (Map.Entry<DataSegment, Integer> entry : popularSegments.entrySet())
		{
			DataSegment segment = entry.getKey();
			int segmentCount = entry.getValue();
			if(segmentCount!=0){
				this.CCAMap.put(segment, Long.valueOf(segmentCount));
			}
			else{
				this.CCAMap.put(segment, 1L);
			}

			
			/*if (weightedAccessCounts.containsKey(segment) == false)
				weightedAccessCounts.put(segment, (long)segmentCount+delta > numServers? numServers : (long)segmentCount+delta);
			else
			{
				double popularity = weightedAccessCounts.get(segment).doubleValue();
				weightedAccessCounts.put(segment, ((long)(segmentCount + popularity))> numServers? numServers : (long)(segmentCount + popularity));
			}*/
			weightedAccessCounts.put(segment, (long)segmentCount+delta > numServers? numServers : (long)segmentCount+delta);
			if(weightedAccessCounts.get(segment)==1){
				log.info("Reduce Segment Access [%s] To 1", segment.getInterval());
			}
		}
		
		// Remove segments that doesn't appear in the access window
        /*Iterator it = weightedAccessCounts.entrySet().iterator();
		while (it.hasNext())
		{
            Map.Entry<DataSegment, Long> entry = (Map.Entry<DataSegment, Long>)it.next();
			log.info("Weighted Access Segment [%s] Count [%f]", entry.getKey().getIdentifier(), entry.getValue().doubleValue());
            if(!popularSegments.containsKey(entry.getKey())){
            	log.info("Reduce Segment Access [%s] To 1", entry.getKey());
            	weightedAccessCounts.put(entry.getKey(), 1L);
            	removeList.put(entry.getKey(), (long)(params.getSegmentReplicantLookup().getTotalReplicants(entry.getKey().getIdentifier())-1));
            }
		}*/
	}

	
	
	private void calculateScarlettReplication(DruidCoordinatorRuntimeParams params, 
			HashMap<DataSegment, Long> weightedAccessCounts, 
			HashMap<DataSegment, List<DruidServerMetadata>> currentTable,
			HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable, 
			HashMap<DruidServerMetadata, Double> nodeVolumes)
	{
		log.info("Calculating Scarlett Replication for Segments");
		int historicalNodeCount = 0;
		Map<DruidServerMetadata, Double> sortedNodeCapacities = new HashMap<DruidServerMetadata, Double>();
		Map<DruidServerMetadata, Double> sortedNodeCapacitiesReverse = new HashMap<DruidServerMetadata, Double>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
			historicalNodeCount += serverQueue.size(); 


		HashMap<ImmutableDruidServer, ServerHolder> serverHolderMap = new HashMap<ImmutableDruidServer, ServerHolder>();
		for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier())
            for (ServerHolder holder : serverQueue)
			    serverHolderMap.put(holder.getServer(), holder);
		
		if (serverHolderMap.size() == 0) {
			//log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
			return;
		}

		
		//HashMap<DataSegment, List<DruidServerMetadata>> currentTable = new HashMap<DataSegment, List<DruidServerMetadata>>();		
		for (ImmutableDruidServer server : serverHolderMap.keySet())
		{
			for (DataSegment segment : server.getSegments().values())
			{
                //log.info("Server [%s] has segment [%s]", server.getHost(), segment.getIdentifier());
				if (!currentTable.containsKey(segment))
					currentTable.put(segment, new ArrayList<DruidServerMetadata>());
				currentTable.get(segment).add(server.getMetadata());
			}
			
			if(!nodeVolumes.containsKey(server.getMetadata())){
				nodeVolumes.put(server.getMetadata(), 0.0);	
			}
		}

		printCurrentTable(currentTable);

		for(Map.Entry<DataSegment, Long> entry : weightedAccessCounts.entrySet()){
			DataSegment targetSegment = entry.getKey();
			Long repFactor = entry.getValue();
			if(currentTable.containsKey(targetSegment)){
				sortedNodeCapacities = sortByValue(nodeVolumes, true);
				List<DruidServerMetadata> currentLocations = currentTable.get(targetSegment);
				
				if(currentLocations.size()<repFactor){//if we have less replicas than we need					
					int count = currentLocations.size();
					HashMap<DruidServerMetadata, Long> valuelist = new HashMap<DruidServerMetadata, Long>();
					
					for(Map.Entry<DruidServerMetadata, Double> pair: sortedNodeCapacities.entrySet()){
						if(!currentLocations.contains(pair.getKey())){
							Long cca = this.CCAMap.get(targetSegment);
							nodeVolumes.put(pair.getKey(), (double) (pair.getValue()+cca/(double)(count)));
							count++;
							valuelist.put(pair.getKey(), 0L);
							if(count>=repFactor)
								break;
						}
					}
					
					//also add current location to the routing list
					String logentry = "";
					for(DruidServerMetadata currentLocation : currentLocations){
						valuelist.put(currentLocation, 0L);
						logentry = logentry + currentLocation.getHost()+".";
					}
					logentry = "1. Adding target segment " + targetSegment.getIdentifier() + " with value: " + logentry + " to routing table";
					//log.info(logentry);
					routingTable.put(targetSegment, valuelist); 
				}
				else if(currentLocations.size()>repFactor){//if we have more replicas than we need
					sortedNodeCapacitiesReverse = sortByValue(nodeVolumes, false);
					int count = currentLocations.size();
					HashMap<DruidServerMetadata, Long> valuelist = new HashMap<DruidServerMetadata, Long>();
					
					for(Map.Entry<DruidServerMetadata, Double> pair: sortedNodeCapacitiesReverse.entrySet()){
						if(currentLocations.contains(pair.getKey())) {//current heaviest loaded server that contains this segment
							currentLocations.remove(pair.getKey());
							Long cca = this.CCAMap.get(targetSegment);
							nodeVolumes.put(pair.getKey(), (double) (pair.getValue() - cca / (double) count));
							count--;
							log.info("remove segment [%s] from node [%s]", targetSegment.getIdentifier(), pair.getKey().getHost());
							if (count <= repFactor)
								break;
						}
					}
					
					//add the rest of the current loaction to the routing list
					String logentry = "";
					for(DruidServerMetadata currentLocation : currentLocations){
						valuelist.put(currentLocation, 0L);
						logentry = logentry + currentLocation.getHost()+".";
					}
					logentry = "2. Adding target segment " + targetSegment.getIdentifier() + " with value: " + logentry + " to routing table";
					//log.info(logentry);
					routingTable.put(targetSegment, valuelist);
				}
				
			}else{//segments is not currently stored
				//create all entries in routing table
				sortedNodeCapacities = sortByValue(nodeVolumes, true);
				int count = 0;
				HashMap<DruidServerMetadata, Long> valuelist = new HashMap<DruidServerMetadata, Long>();
				String logentry = "";
				for(Map.Entry<DruidServerMetadata, Double> pair:sortedNodeCapacities.entrySet()){
					Long cca = this.CCAMap.get(targetSegment);	
					nodeVolumes.put(pair.getKey(), (double) (pair.getValue()+cca/(double)(count+1)));
					count++;
					valuelist.put(pair.getKey(), 0L);
					logentry = logentry + pair.getKey().getHost()+".";
					if(count>=repFactor)
						break;
				}
				routingTable.put(targetSegment, valuelist);
				logentry = "3. Creating target segment " + targetSegment.getIdentifier() + " with value: " + logentry + " to routing table";
				//log.info(logentry);
			}
		}
	}

	private void manageReplicas(DruidCoordinatorRuntimeParams params, HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable, CoordinatorStats stats, HashMap<DataSegment, List<DruidServerMetadata>> currentTable)
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
		
		//HashMap<DataSegment, List<DruidServerMetadata>> currentTable = new HashMap<DataSegment, List<DruidServerMetadata>>();		
		for (ImmutableDruidServer server : serverHolderMap.keySet())
		{
			for (DataSegment segment : server.getSegments().values())
			{
                //log.info("Server [%s] has segment [%s]", server.getHost(), segment.getIdentifier());
				if (!currentTable.containsKey(segment))
					currentTable.put(segment, new ArrayList<DruidServerMetadata>());
				currentTable.get(segment).add(server.getMetadata());
			}
		}

		final List<String> tierNameList = Lists.newArrayList(params.getDruidCluster().getTierNames());
		if (tierNameList.size() == 0) { //!!!!!!!!!!!!!!!!!!!!!!CHECK LATER
			//log.makeAlert("Cluster has multiple tiers! Check your cluster configuration!").emit();
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
					//log.info("Server [%s] add segment [%s]", server.getHost(), segment.getIdentifier());
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
		
		for (Map.Entry<DataSegment, List<DruidServerMetadata>> entry : currentTable.entrySet())
		{
			DataSegment segment = entry.getKey();
			for (DruidServerMetadata server : entry.getValue())
			{
				if (!routingTable.containsKey(segment) || !routingTable.get(segment).keySet().contains(server))
				{
					log.info("Server [%s] drop segment [%s]", server.getHost(), segment.getIdentifier());
					CoordinatorStats dropStats = DruidCoordinatorReplicatorHelper.drop(
							replicatorThrottler,
							tier,
							serverMetaDataMap.get(server),
							segment
							);
					stats.accumulate(dropStats); 
				}
			}
		}
	}

	//code referenced: http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java?noredirect=1&lq=1
	//http://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
	private static Map<DruidServerMetadata, Double> sortByValue(HashMap<DruidServerMetadata, Double> unsortMap, final boolean order)
    {

        List<Map.Entry<DruidServerMetadata, Double>> list = new LinkedList<Map.Entry<DruidServerMetadata, Double>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Map.Entry<DruidServerMetadata, Double>>()
        {
            public int compare(Map.Entry<DruidServerMetadata, Double> o1,
                    Map.Entry<DruidServerMetadata, Double> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }

			
        });

        // Maintaining insertion order with the help of LinkedList
        Map<DruidServerMetadata, Double> sortedMap = new LinkedHashMap<DruidServerMetadata, Double>();
        for (Map.Entry<DruidServerMetadata, Double> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}
