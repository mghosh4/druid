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
import io.druid.query.SegmentDescriptor;
import io.druid.server.coordination.broker.DruidBroker;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.io.IOException;

public class GetafixServerSelectorStrategy implements ServerSelectorStrategy
{
  private static final EmittingLogger log = new EmittingLogger(GetafixServerSelectorStrategy.class);

  @JacksonInject
  DruidBroker druidBroker;

  @JacksonInject
  ServerDiscoveryFactory serverDiscoveryFactory;

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment, SegmentDescriptor segmentDescriptor, String queryType)
  {
    if (servers.size() == 0) {
      log.error("[GETAFIX ROUTING] No QueryableDruidServers in the set");
      return null;
    }

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
