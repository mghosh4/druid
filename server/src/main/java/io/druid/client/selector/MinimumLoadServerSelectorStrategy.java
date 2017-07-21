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

  private static final Comparator<QueryableDruidServer> loadcomparator = new Comparator<QueryableDruidServer>()
  {
    @Override
    public int compare(QueryableDruidServer left, QueryableDruidServer right)
    {
      return Longs.compare(left.getServer().getCurrentLoad(), right.getServer().getCurrentLoad());
    }
  };

  private static final Comparator<QueryableDruidServer> loadncccomparator = new Comparator<QueryableDruidServer>()
  {
    @Override
    public int compare(QueryableDruidServer left, QueryableDruidServer right)
    {
      return Longs.compare(left.getServer().getCurrentLoad() + left.getClient().getNumOpenConnections(), 
                            right.getServer().getCurrentLoad() +  right.getClient().getNumOpenConnections());
    }
  };

  // Pick() the minimum loaded server as per a probability distribution
  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
      if (servers.size() == 1)
          return servers.iterator().next();

      List<QueryableDruidServer> serverList = new ArrayList<QueryableDruidServer>();
      for (QueryableDruidServer server : servers) {
          if (!server.getServer().getMetadata().getType().equals("realtime")) {
              serverList.add(server);
          }
      }
      return Collections.min(serverList, loadcomparator);
  }
}
