package io.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.collect.Iterators;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.server.coordination.broker.DruidBroker;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class GetafixQueryTimeServerSelectorStrategy implements ServerSelectorStrategy
{
  private static final EmittingLogger log = new EmittingLogger(GetafixQueryTimeServerSelectorStrategy.class);

  @JacksonInject
  DruidBroker druidBroker;

  @JacksonInject
  ServerDiscoveryFactory serverDiscoveryFactory;

  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    if (servers.size() == 0) {
      log.error("[GETAFIX ROUTING] No QueryableDruidServers in the set");
      return null;
    }

    ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> allocationMap = druidBroker.getAllocationTable();
    Map<String, Map<String, Long>> routingTable = druidBroker.getRoutingTable();

    Map<String, Long> hnList = routingTable.get(segment.getIdentifier());
    if (hnList != null) {
      String chosenServer;
      //make sure both segment and server mapping exist
      if(allocationMap.containsKey(segment.getIdentifier())){
        //allocationMap has segment info
        ConcurrentHashMap<String, Double> allocation = allocationMap.get(segment.getIdentifier());
        if(allocation.size()!=hnList.size()){
          for(Map.Entry<String, Long> entry : hnList.entrySet()){
            if(!allocation.containsKey(entry.getKey())){
              allocation.put(entry.getKey(), 0.0);
            }
          }
        }
        //update original map
        allocationMap.put(segment.getIdentifier(), allocation);
        chosenServer = GetafixQueryTimeServerSelectorStrategyHelper.compareAndSelect(allocation, hnList);
      }
      else{
        //allocationMap has no segment info
        ConcurrentHashMap<String, Double> allocation = new ConcurrentHashMap<>();
        for(Map.Entry<String, Long> entry:hnList.entrySet()){
          allocation.put(entry.getKey(), 0.0);
        }
        //update original map
        allocationMap.put(segment.getIdentifier(), allocation);
        chosenServer = GetafixQueryTimeServerSelectorStrategyHelper.compareAndSelect(allocation, hnList);
      }
      if (chosenServer == null) {
        log.error("[GETAFIX ROUTING] Cannot find a server in routingTable to match");
        return null;
      }

      log.info("Chosen server: " + chosenServer);
      for (QueryableDruidServer server : servers) {
        log.info("Looking at QueryableDruidServer: " + server.getServer().getMetadata().toString());
        if (chosenServer.equals(server.getServer().getMetadata().toString())) {
          log.info("[GETAFIX ROUTING] SUCCESS");
          return server;
        }
      }
    }

    if (servers.size() > 0){
      QueryableDruidServer server = Iterators.get(servers.iterator(), ThreadLocalRandom.current().nextInt(servers.size()));
      return server;
    }


    log.error("[GETAFIX ROUTING] Trying to load segment on demand");
    String druidServerMetadata = loadSegmentOnDemand(segment);
    if (druidServerMetadata == null) {
      log.error("[GETAFIX ROUTING] Cannot find even with loading on demand");
      return null;
    }

    for (QueryableDruidServer server : servers) {
      if (druidServerMetadata.equals(server.getServer().getMetadata().toString())) {
        log.info("[GETAFIX ROUTING] SUCCESS");
        return server;
      }
    }

    return null;
  }

  private String loadSegmentOnDemand(DataSegment segment) {
    String coordinatorService = TieredBrokerConfig.DEFAULT_COORDINATOR_SERVICE_NAME;
    ServerDiscoverySelector selector = serverDiscoveryFactory.createSelector(coordinatorService);

    try {
      selector.start();
      Server coordinator = selector.pick();
      URI uri = new URI(
          coordinator.getScheme(),
          null,
          coordinator.getAddress(),
          coordinator.getPort(),
          "/druid/coordinator/v1/loadSegment/" + segment.getIdentifier(),
          null,
          null
      );

      String url = uri.toString();

      StatusResponseHolder response = druidBroker.getHttpClient().go(
          new Request(
              HttpMethod.POST,
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

      String resp = response.getContent();
      log.info("[GETAFIX PLACEMENT] Load segment " + segment.getIdentifier() + " on demand. " + resp);
      selector.stop();
      return resp;
    } catch (Exception e) {
      log.error("[GETAFIX PLACEMENT] On demand loading error: " + e.getMessage());
      try
      {
        selector.stop();
      }
      catch(IOException ex)
      {
        log.error("[GETAFIX PLACEMENT] Error stopping selector");
      }
      return null;
    }
  }
}
