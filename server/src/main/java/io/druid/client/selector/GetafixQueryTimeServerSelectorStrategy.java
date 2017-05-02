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
import java.util.concurrent.ThreadLocalRandom;

public class GetafixQueryTimeServerSelectorStrategy implements ServerSelectorStrategy
{
  private static final EmittingLogger log = new EmittingLogger(GetafixQueryTimeServerSelectorStrategy.class);

  @JacksonInject
  DruidBroker druidBroker;

  @JacksonInject
  ServerDiscoveryFactory serverDiscoveryFactory;

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

  //Loading path
  String loadingPath = "/proj/ISS/lexu/distribution/";
  String groupbyPath = loadingPath+"groupby.cdf";
  String timeseriesPath = loadingPath+"timeseries.cdf";
  String topnPath = loadingPath+"topn.cdf";

  HashMap<String, double[]> percentileCollection;


  String[] queryTypes = {"groupby", "timeseries", "topn"};
  /*

  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    if (servers.size() == 0) {
      log.error("[GETAFIX ROUTING] No QueryableDruidServers in the set");
      return null;
    }

    HashMap<String, ArrayList<Double>> percentileCollection = druidBroker.getPercentileCollection();
    HashMap<String, HashMap<Double, Double>> histogramCollection = druidBroker.getHistogramCollection();


    Map<String, Map<String, Long>> routingTable = druidBroker.getRoutingTable();
    Map<String, Long> hnList = routingTable.get(segment.getIdentifier());
    if (hnList != null) {
        long N = 0L;
        for (Map.Entry<String, Long> hnPair : hnList.entrySet()) {
          N += hnPair.getValue();
        }

        long randCounter = ThreadLocalRandom.current().nextLong(N);
        String chosenServer = null;
        for (Map.Entry<String, Long> hnPair : hnList.entrySet()) {
          randCounter -= hnPair.getValue();
          if (randCounter < 0) {
            chosenServer = hnPair.getKey();
            break;
          }
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

    if (servers.size() > 0)
        return Iterators.get(servers.iterator(), ThreadLocalRandom.current().nextInt(servers.size()));

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
*/
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    if (servers.size() == 0) {
      log.error("[GETAFIX ROUTING] No QueryableDruidServers in the set");
      return null;
    }

    HashMap<String, ArrayList<Double>> percentileCollection = druidBroker.getPercentileCollection();
    HashMap<String, HashMap<Double, Double>> histogramCollection = druidBroker.getHistogramCollection();
    HashMap<String, HashMap<String, Double>> allocationMap = druidBroker.getAllocationTable();


    Map<String, Map<String, Long>> routingTable = druidBroker.getRoutingTable();



    Map<String, Long> hnList = routingTable.get(segment.getIdentifier());
    if (hnList != null) {
      String chosenServer="";
      //make sure both segment and server mapping exist
      if(allocationMap.containsKey(segment.getIdentifier())){
        //allocationMap has segment info
        HashMap<String, Double> allocation = allocationMap.get(segment.getIdentifier());
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
        HashMap<String, Double> allocation = new HashMap<>();
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

      //allocationMap
      //update the table before return
      updateAllocationTable(chosenServer, histogramCollection, percentileCollection, segment, allocationMap);


      log.info("Chosen server: " + chosenServer);
      for (QueryableDruidServer server : servers) {
        log.info("Looking at QueryableDruidServer: " + server.getServer().getMetadata().toString());
        if (chosenServer.equals(server.getServer().getMetadata().toString())) {
          log.info("[GETAFIX ROUTING] SUCCESS");
          updateAllocationTable(chosenServer, histogramCollection, percentileCollection, segment, allocationMap);
          return server;
        }
      }
    }

    if (servers.size() > 0){
      QueryableDruidServer server = Iterators.get(servers.iterator(), ThreadLocalRandom.current().nextInt(servers.size()));
      updateAllocationTable(server.getServer().getMetadata().toString(), histogramCollection, percentileCollection, segment, allocationMap);
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
        updateAllocationTable(server.getServer().getMetadata().toString(), histogramCollection, percentileCollection, segment, allocationMap);
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

  private void updateAllocationTable(String chosenServer, HashMap<String, HashMap<Double, Double>> histogramCollection, HashMap<String, ArrayList<Double>> percentileCollection, DataSegment segment, HashMap<String, HashMap<String, Double>> allocationMap) {
    if(!allocationMap.containsKey(segment.getIdentifier())){
      HashMap<String, Double> allocation = new HashMap<>();
      allocation.put(chosenServer, 0.0);
      allocationMap.put(segment.getIdentifier(), allocation);
    }
    if(!allocationMap.get(segment.getIdentifier()).containsKey(chosenServer)){
      allocationMap.get(segment.getIdentifier()).put(chosenServer, 0.0);
    }
    double oldallocation = allocationMap.get(segment.getIdentifier()).get(chosenServer);
    double estimated_weight = GetafixQueryTimeServerSelectorStrategyHelper.selectRandomQueryTime(histogramCollection.get("timeseries"), percentileCollection.get("timeseries"));
    log.info("[GETAFIX ROUTING] adding weight to allocation [%s]", estimated_weight);
    double newallocation = oldallocation + estimated_weight;
    allocationMap.get(segment.getIdentifier()).put(chosenServer, newallocation);
    druidBroker.setAllocationTable(allocationMap);
  }
}
