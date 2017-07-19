package io.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.MutablePair;
import io.druid.query.SegmentDescriptor;
import io.druid.server.coordination.broker.DruidBroker;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Random;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Date;
import java.lang.Math;

// RML - Replication aware Minimum Load
public class RMLServerSelectorStrategy implements ServerSelectorStrategy
{
    private static final EmittingLogger log = new EmittingLogger(RMLServerSelectorStrategy.class);

    @JacksonInject
    DruidBroker druidBroker;

    @JacksonInject
    ServerDiscoveryFactory serverDiscoveryFactory;

    private final ObjectMapper jsonMapper = new DefaultObjectMapper();
    private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

    private static final Comparator<QueryableDruidServer> comparator = new Comparator<QueryableDruidServer>()
    {
        @Override
        public int compare(QueryableDruidServer left, QueryableDruidServer right)
        {
            return Ints.compare(left.getClient().getNumOpenConnections(), right.getClient().getNumOpenConnections());
        }
    };

    private static final Comparator<QueryableDruidServer> loadcomparator = new Comparator<QueryableDruidServer>()
    {
        @Override
        public int compare(QueryableDruidServer left, QueryableDruidServer right)
        {
            return Longs.compare(left.getServer().getCurrentLoad(), right.getServer().getCurrentLoad());
        }
    };

    // Pick() the minimum loaded server as per a probability distribution
    @Override
    public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment, SegmentDescriptor segmentDescriptor, String queryType)
    {
        QueryableDruidServer chosenServer = null;
        float maxValue = -1;
        float minValue = 1000000;
        int minValueIndex = -1;
        int counter = 0;
        List<QueryableDruidServer> serverList = new ArrayList<QueryableDruidServer>();

        Map<String, Long> segmentRoutingTable = druidBroker.getRoutingTable().get(segment.getIdentifier());

        // get the server IDs under consideration
        long firstHNValue = -1;
        long firstQueryTimeAllocationValue = -1;
        int i =0;
        float maxDeltaRatio = -Float.MAX_VALUE;
        for (Iterator<QueryableDruidServer> iterator = servers.iterator(); iterator.hasNext();) {
            QueryableDruidServer s = iterator.next();
            boolean remove = true;
            for(Map.Entry<String, Long> entry : segmentRoutingTable.entrySet()) {
                if (s.getServer().getMetadata().toString().equals(entry.getKey())) {
                    remove = false;
                    break;
                }
            }
            if (remove == true){
                log.info("Removed server %s type %s from list", s.getServer().getMetadata().getName(), s.getServer().getMetadata().getType());
                iterator.remove();
                continue;
            }
            String hn = s.getServer().getMetadata().toString();

            //log.info("Queryable server %s, allocation %d", s.getServer().getMetadata().getHost(), hnQueryTimeAllocation.get(hn));
            //log.info("Goal value %d, Current value %d", firstHNValue, firstQueryTimeAllocationValue);
            if (firstHNValue == -1) {
                maxDeltaRatio = 0;
                chosenServer = s;
                firstHNValue = segmentRoutingTable.get(hn);
                firstQueryTimeAllocationValue = s.getServer().getCurrentLoad() + s.getClient().getNumOpenConnections();
//                log.info("First Modified allotment hn %s, segment %s, allocation %d, tasks %d, threads %d, modified allocation %d",
//                        hn, segment.getIdentifier(), allocation, numSegmentTasks, numHnThreadsAllottedToSegment, firstQueryTimeAllocationValue);
                //log.info("Ratio comparison hn %s goalRatio 1.0, currentRatio 1.0, deltaRatio 0.0, maxDeltaRatio 0.0, chosenServer %s",
                // s.getServer().getMetadata().getName(), s.getServer().getMetadata().getName());
            }
            else{
                float goalRatio = (float)segmentRoutingTable.get(hn)/(float)firstHNValue;
                Long allocation = s.getServer().getCurrentLoad() + s.getClient().getNumOpenConnections();
//                log.info("Modified allotment hn %s, segment %s, allocation %d, tasks %d, threads %d, modified allocation %d",
//                        hn, segment.getIdentifier(), allocation, numSegmentTasks, numHnThreadsAllottedToSegment, modifiedAllocation);
                float currentRatio = (float)allocation/(float)firstQueryTimeAllocationValue;
                float deltaRatio =  goalRatio - currentRatio;
                if(deltaRatio > maxDeltaRatio){
                    maxDeltaRatio = deltaRatio;
                    chosenServer = s;
                }
                //log.info("Ratio comparison hn %s goalRatio %.4f, currentRatio %.4f, deltaRatio %.4f, maxDeltaRatio %.4f, chosenServer %s",
                // s.getServer().getMetadata().getName(), goalRatio, currentRatio, deltaRatio, maxDeltaRatio, s.getServer().getMetadata().getName());
            }
            i++;
        }

        if (chosenServer == null) {
            log.error("Error: cannot find a server in routingTable to match");
            return null;
        }
        else {
            return chosenServer;
        }
    }
}

