package io.druid.server.coordination.broker.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Iterables;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.ServerInventoryView;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
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
  private final ServerInventoryView<Object> serverView;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

  public PeriodicPollHistoricalLoad(ServerInventoryView serverView, HttpClient httpClient)
  {
    this.serverView = serverView;
    this.httpClient = httpClient;
  }

  @Override
  public void run()
  {
    int serverSize = Iterables.size(serverView.getInventory());
    if (serverSize == 0)
        return;

    ExecutorService pool = Executors.newFixedThreadPool(serverSize);
    List<Future> futures = new ArrayList<Future>();

    for (final DruidServer server : serverView.getInventory())
    {
      if (!server.getType().equalsIgnoreCase("historical"))
        continue;

      // Should use threads to fetch in parallel from all brokers
      URI uri = null;
      try {
        uri = new URI(
                "http",
                null,
                server.getHost().split(":")[0],
                Integer.parseInt(server.getHost().split(":")[1]),
                "/druid/historical/v1/currentLoad",
                null,
                null);
      } catch (URISyntaxException e) {
        log.warn("URI did not get created [%s]", server.getHost());
        // TODO Auto-generated catch block
        e.printStackTrace();
        continue;
      }

      final String url = uri.toString();
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

            currentLoadMap = jsonMapper.readValue(
                    response.getContent(), new TypeReference<Map<String, Long>>()
                    {
                    }
            );
          }
          catch (Exception e) {
            log.warn("Something failed [%s]", e.toString());
            e.printStackTrace();
          }

          long serverLoad = 0;
          try
          {
            serverLoad = currentLoadMap.get("currentload");
          }
          catch (Exception e)
          {
            log.warn("No key existing called currentload");
          }

          if (serverLoad > 0)
            log.debug("Current Server Load [%d]", serverLoad);
          
          server.setCurrentLoad(serverLoad);
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
