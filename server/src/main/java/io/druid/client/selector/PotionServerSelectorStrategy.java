package io.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.SegmentDescriptor;
import io.druid.server.coordination.broker.DruidBroker;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class PotionServerSelectorStrategy implements ServerSelectorStrategy
{
    private static final EmittingLogger log = new EmittingLogger(PotionServerSelectorStrategy.class);

    @JacksonInject
    DruidBroker druidBroker;

    @JacksonInject
    ServerDiscoveryFactory serverDiscoveryFactory;

    private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

    private static final Comparator<QueryableDruidServer> comparator = new Comparator<QueryableDruidServer>()
    {
        @Override
        public int compare(QueryableDruidServer left, QueryableDruidServer right)
        {
            return Ints.compare(left.getClient().getNumOpenConnections(), right.getClient().getNumOpenConnections());
        }
    };

    public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment, SegmentDescriptor segmentDescriptor, String queryType)
    {
        if (servers.size() == 0) {
            log.error("No QueryableDruidServers in the set");
            return null;
        }
        log.info("Got segment %s numQueryableServers %d", segment.getInterval(), servers.size());
        
        // keep a copy of servers, used in case no server is selected based on allocation
        List<QueryableDruidServer> serversBackup = new ArrayList<QueryableDruidServer>(servers);

        Map<String, Long> segmentRoutingTable = druidBroker.getRoutingTable().get(segment.getIdentifier());
        ConcurrentHashMap<String, Long> hnQueryTimeAllocation = druidBroker.getHNQueryTimeAllocation();
        // get the query runtime estimate
        long queryRuntimeEstimate = druidBroker.getQueryRuntimeEstimate(queryType, segmentDescriptor.getInterval().toDurationMillis());
        log.info("Query run time estimate for queryType %s, duration %d, estimate %d",
                queryType, segmentDescriptor.getInterval().toDurationMillis(), queryRuntimeEstimate);

        // if routing table is not setup or no estimate is available for the current query, then route it with connection count
        if(segmentRoutingTable == null || queryRuntimeEstimate == 0){
            log.info("Routing query using connection count queryRuntimeEstimate %d, servers size %d", queryRuntimeEstimate, servers.size());
            return Collections.min(serversBackup, comparator);
        }

        // filter out HNs for which the routing table allocation is 0 or if the HN is not present in the routing table
        for (Iterator<QueryableDruidServer> iterator = servers.iterator(); iterator.hasNext();) {
            QueryableDruidServer s = iterator.next();
            boolean remove = true;
            for(Map.Entry<String, Long> entry : segmentRoutingTable.entrySet()) {
                if(s.getServer().getMetadata().toString().equals(entry.getKey())){
                    if (entry.getValue() != 0) {
                        remove = false;
                    }
                    else{
                        log.info("Removed server %s from list due to 0 routing table allocation", entry.getKey());
                    }
                    break;
                }
            }
            if (remove == true){
                log.info("Removed server %s type %s from list", s.getServer().getMetadata().getName(), s.getServer().getMetadata().getType());
                iterator.remove();
            }
        }

        // if no servers are left, then route on backup servers with connection count
        if(servers.size() == 0){
            log.info("Routing query using connection count (no servers left) queryRuntimeEstimate %d, servers size %d", queryRuntimeEstimate, servers.size());
            return Collections.min(serversBackup, comparator);
        }
 
        log.info("Segment routing table %s", segmentRoutingTable.toString());
        log.info("Queryable druid servers %s", servers.toString());
        
        // get the server IDs under consideration
        //String [] candidateHNList = new String[servers.size()];
        //float [] goalRatio = new float[servers.size()];
        //float [] currentRatio = new float[servers.size()];
        long firstHNValue = -1;
        long firstQueryTimeAllocationValue = -1;
        int i =0;
        float maxDeltaRatio = -Float.MAX_VALUE;
        QueryableDruidServer chosenServer = null;
        for(QueryableDruidServer s : servers){
            //candidateHNList[i] = s.getServer().getMetadata().toString();
            String hn = s.getServer().getMetadata().toString();
            if(hnQueryTimeAllocation.get(hn) == null){
                hnQueryTimeAllocation.put(hn, 1L); // initialize with 1 to avoid div by 0 errors
            }
            log.info("Queryable server %s, allocation %d", s.getServer().getMetadata().getHost(), hnQueryTimeAllocation.get(hn));
            log.info("Goal value %d, Current value %d", firstHNValue, firstQueryTimeAllocationValue);
            if (firstHNValue == -1) {
                //goalRatio[i] = 1.0F;
                maxDeltaRatio = 0;
                chosenServer = s;
                firstHNValue = segmentRoutingTable.get(hn);
                firstQueryTimeAllocationValue = hnQueryTimeAllocation.get(hn);
                log.info("Ratio comparison hn %s goalRatio 1.0, currentRatio 1.0, deltaRatio 0.0, maxDeltaRatio 0.0, chosenServer %s",
                        s.getServer().getMetadata().getName(), s.getServer().getMetadata().getName());
            }
            else{
                //goalRatio[i] = segmentRoutingTable.get(hn)/firstHNValue;
                //currentRatio[i] = hnQueryTimeAllocation.get(hn)/firstQueryTimeAllocationValue;
                float goalRatio = (float)segmentRoutingTable.get(hn)/(float)firstHNValue;
                float currentRatio = (float)hnQueryTimeAllocation.get(hn)/(float)firstQueryTimeAllocationValue;
                float deltaRatio =  goalRatio - currentRatio;
                if(deltaRatio > maxDeltaRatio){
                    maxDeltaRatio = deltaRatio;
                    chosenServer = s;
                }
                log.info("Ratio comparison hn %s goalRatio %.4f, currentRatio %.4f, deltaRatio %.4f, maxDeltaRatio %.4f, chosenServer %s",
                        s.getServer().getMetadata().getName(), goalRatio, currentRatio, deltaRatio, maxDeltaRatio, s.getServer().getMetadata().getName());
            }
            i++;
        }

        // update the hnQueryTimeAllocation table
        long newAllocation = hnQueryTimeAllocation.get(chosenServer.getServer().getMetadata().toString()) + queryRuntimeEstimate;
        hnQueryTimeAllocation.put(chosenServer.getServer().getMetadata().toString(), newAllocation);
        log.info("Queryable server %s newAllocation %d", chosenServer.getServer().getMetadata().getName(), newAllocation);

        log.info("DataSegment interval %s, version %s, partition %d, runtimeEstimate %d",
                segment.getInterval(), segment.getVersion(), segment.getShardSpec().getPartitionNum(), queryRuntimeEstimate);

        if (chosenServer == null) {
            log.error("Error: cannot find a server in routingTable to match");
            return null;
        }
        else {
            return chosenServer;
        }
    }
}
