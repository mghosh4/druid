package io.druid.server.coordination;

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
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.router.TieredBrokerConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.Date;

/**
 * Created by raina4 on 06/21/17.
 */
public class PeriodicLoadUpdate implements Runnable
{
  private static final EmittingLogger log = new EmittingLogger(PeriodicLoadUpdate.class);
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final ServerDiscoveryFactory serverDiscoveryFactory;
  private final ServerManager serverManager;
  private final HttpClient httpClient;
  private final StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  List<URI> brokerURIs;

  public PeriodicLoadUpdate(ServerManager serverManager, ServerDiscoveryFactory serverDiscoveryFactory)
  {
    this.serverDiscoveryFactory = serverDiscoveryFactory;
    this.serverManager = serverManager;
    this.httpClient = serverManager.httpClient;
    genBrokerURIs();
  }

  /* generates broker URIs once at class instance creation time */
  private void genBrokerURIs()
  {
    String brokerservice = TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME;
    ServerDiscoverySelector selector = serverDiscoveryFactory.createSelector(brokerservice);
    try {
      selector.start();
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    Collection<Server> brokers = selector.getAll();

    brokerURIs = new ArrayList<URI>(brokers.size());
    for (Server broker : brokers)
    {
      // Should use threads to fetch in parallel from all brokers
      URI uri = null;
      try {
        uri = new URI(
                broker.getScheme(),
                null,
                broker.getAddress(),
                broker.getPort(),
                "/druid/broker/v1/hnload",
                null,
                null);
      } catch (URISyntaxException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      log.info("URI [%s]", uri.toString());

      //uris.add(uri.toString());
      brokerURIs.add(uri);
    }
    log.info("Number of Broker Servers [%d]", brokers.size());
  }

  @Override
  public void run()
  {
    int numBrokers = brokerURIs.size();
    if(numBrokers == 0){
      genBrokerURIs();
      numBrokers = brokerURIs.size();
      if(numBrokers == 0) {
        log.info("No broker URIs found");
        return;
      }
    }

    ExecutorService pool = Executors.newFixedThreadPool(numBrokers);
    List<Future> futures = new ArrayList<Future>();

    for (final URI uri : brokerURIs)
    {
      final String url = uri.toString();
      futures.add(pool.submit(new Callable<Void>(){
        @Override
        public Void call()
        {
          Map<String, Long> currentLoadMap = Maps.newHashMap();
          try {
            log.info("Sending hnLoad POST message");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            StatusResponseHolder response = httpClient.go(
                    new Request(
                            HttpMethod.POST,
                            new URL(url)
                    ).setContent(MediaType.TEXT_PLAIN, (serverManager.currentHNLoad()+"_"+sdf.format(new Date())).getBytes()),
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
            else{
              log.info("Received OK for HN load POST");
            }
          }
          catch (Exception e) {
            log.warn("Something failed [%s]", e.toString());
            e.printStackTrace();
          }
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
