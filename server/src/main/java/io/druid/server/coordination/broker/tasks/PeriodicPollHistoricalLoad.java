package io.druid.server.coordination.broker.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.broker.DruidBroker;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by mghosh4 on 10/12/16.
 */
public class PeriodicPollHistoricalLoad implements Runnable
{
  private static final EmittingLogger log = new EmittingLogger(PeriodicPollHistoricalLoad.class);
  private final DruidBroker druidBroker;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  private final ConcurrentMap<String, QueryableDruidServer> servers;

  public PeriodicPollHistoricalLoad(DruidBroker druidBroker, HttpClient httpClient)
  {
    this.druidBroker = druidBroker;
    this.httpClient = httpClient;

    this.servers = druidBroker.getServerView().getServerMap();
  }

  @Override
  public void run()
  {
    ExecutorService pool = Executors.newFixedThreadPool(servers.size());
    List<Future> futures = new ArrayList<Future>();

    for (final QueryableDruidServer server : servers.values())
    {
      if (!server.getServer().getType().equalsIgnoreCase("historical"))
        continue;

      // Should use threads to fetch in parallel from all brokers
      URI uri = null;
      try {
        uri = new URI(
                "http",
                null,
                server.getServer().getHost().split(":")[0],
                Integer.parseInt(server.getServer().getHost().split(":")[1]),
                "/druid/historical/v1/currentLoad",
                null,
                null);
      } catch (URISyntaxException e) {
        log.warn("URI did not get created [%s]", server.getServer().getHost());
        // TODO Auto-generated catch block
        e.printStackTrace();
        continue;
      }

      final String url = uri.toString();
      log.info("URI [%s]", url);

      futures.add(pool.submit(new Callable<Void>(){
        @Override
        public Void call()
        {
          Map<String, Long> currentLoadMap = Maps.newHashMap();
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

            currentLoadMap = jsonMapper.readValue(
                    response.getContent(), new TypeReference<Map<String, Long>>()
                    {
                    }
            );
          }
          catch (Exception e) {
            e.printStackTrace();
          }

          server.setCurrentLoad(currentLoadMap.get("currentload"));
          return null;
        }
      }));
    }

    for (Future future: futures)
    {
      try {
        future.get();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}
