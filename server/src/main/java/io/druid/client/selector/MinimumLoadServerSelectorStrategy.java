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
import java.util.concurrent.ThreadLocalRandom;
import java.util.Date;
import java.lang.Math;

public class MinimumLoadServerSelectorStrategy implements ServerSelectorStrategy
{
  private static final EmittingLogger log = new EmittingLogger(MinimumLoadServerSelectorStrategy.class);

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

/*
  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    return Collections.min(servers, comparator);
  }
*/

  // Pick() the minimum loaded server as per a probability distribution
  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment, SegmentDescriptor segmentDescriptor, String queryType)
  {
      if (servers.size() == 1)
          return servers.iterator().next();

      List<QueryableDruidServer> serverList = new ArrayList<QueryableDruidServer>();
      for (QueryableDruidServer server : servers) {
          if (!server.getServer().getMetadata().getType().equals("realtime")) {
              serverList.add(server);
              // long temp = server.getServer().getCurrentLoad();
              // exponentially decay the load value
              /*
              Date currTime = new Date();
              long refreshTime = (currTime.getTime() - s.getServer().getCurrentLoadTimeAtClient().getTime());
              double decayRate = -0.05;
              double e = 2.7183;
              temp = (long) (temp * Math.pow(e, decayRate * refreshTime));
              log.info("Server name %s, Server host %s, Prev load %d, Decayed load %d, refreshTime %d", s.getServer().getMetadata().getName(), s.getServer().getMetadata().getHost(), s.getServer().getCurrentLoad(), temp, refreshTime);
              */
          }
      }

      // pick the server based on the probability distribution of loads
      /*
      maxLoading++;
      //log.info("Max loading %d", maxLoading);
      long prev = 0;
      for(int i=0; i<loading.size(); i++){
          long value = maxLoading - loading.get(i) + prev;
          loading.set(i, value);
          prev = value;
          //log.info("Frequency %d: %d", i, value);
      }
      // generate a random number from 0 to prev
      Random rn = new Random();
      int randNum = rn.nextInt((int)prev - 0 + 1) + 0;
      //log.info("Random number %d", randNum);
      // loop through loading list to identify the bucket
      int i;
      for(i=0; i<loading.size(); i++){
          long value = loading.get(i);
          if(value >= randNum){
              break;
          }
      }
      //log.info("Selected index %d", i);
      return serverList.get(i);
      */
      //log.info("Selected server name %s, host %s, loading %d", serverList.get(minLoadingIndex).getServer().getMetadata().getName(), serverList.get(minLoadingIndex).getServer().getMetadata().getHost(), loading.get(minLoadingIndex));
      return Collections.min(serverList, loadcomparator);
  }

/*
  // Pick() the minimum loaded server which has few open connections
  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment, SegmentDescriptor segmentDescriptor, String queryType)
  {
    QueryableDruidServer chosenServer = null;

    List<QueryableDruidServer> temp = new ArrayList<QueryableDruidServer>();
    for(QueryableDruidServer s : servers){
      if(s.getClient().getNumOpenConnections() >= 18){
        temp.add(s);
      }
    }

    if(temp.size() < servers.size()){
      //log.info("Ignoring [%d] servers with 20 open connections out of total [%d] servers", temp.size(), servers.size());
      for(QueryableDruidServer t : temp){
        servers.remove(t);
      }
    }

    chosenServer = Collections.min(servers, loadcomparator);

    //log.info("Min loaded HN connections = %d, Max loaded HN connections = %d", chosenServer.getClient().getNumOpenConnections(), Collections.max(servers, loadcomparator).getClient().getNumOpenConnections());

    //String printStr = "";
    //for (QueryableDruidServer s : servers){
      //printStr = printStr+", "+s.getServer().getHost().split("\\.")[0]+":"+String.valueOf(s.getServer().getCurrentLoad());
    //}
    //log.info("Choosing HN [%s] with min load [%d] out of [%d] HNs, HN loading stats [%s]", chosenServer.getServer().getHost().split("\\.")[0], chosenServer.getServer().getCurrentLoad(), servers.size(),  printStr);

    return chosenServer;
  }
*/
}


