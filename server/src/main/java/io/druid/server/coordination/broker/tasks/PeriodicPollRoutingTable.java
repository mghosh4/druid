package io.druid.server.coordination.broker.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.metamx.common.ISE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.broker.DruidBroker;
import io.druid.server.router.TieredBrokerConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiaoyaoqian on 10/12/16.
 */
public class PeriodicPollRoutingTable implements Runnable
{
  private final ServerDiscoveryFactory serverDiscoveryFactory;
  private final DruidBroker druidBroker;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

  public PeriodicPollRoutingTable(DruidBroker druidBroker, ServerDiscoveryFactory serverDiscoveryFactory, HttpClient httpClient)
  {
    this.druidBroker = druidBroker;
    this.serverDiscoveryFactory = serverDiscoveryFactory;
    this.httpClient = httpClient;
  }

  @Override
  public void run()
  {
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
          "/druid/coordinator/v1/routingTable",
          null,
          null);

      String url = uri.toString();

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

      Map<String, Map<String, Long>> routingTable = jsonMapper.readValue(
          response.getContent(),
          new TypeReference<Map<String, Map<String, Long>>>()
          {
          }
      );

      druidBroker.setRoutingTable(routingTable);
      HashMap<String, HashMap<String, Double>> allocationTable = new HashMap<>();
      druidBroker.setAllocationTable(allocationTable);
      selector.stop();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
