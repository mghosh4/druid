/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.BrokerServerView;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.broker.DruidBroker;
import io.druid.timeline.DataSegment;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("/druid/broker/v1")
public class BrokerResource
{
  private final BrokerServerView brokerServerView;
  private final SegmentCollector segmentCollector;
  private final DruidBroker druidBroker;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
    private static final EmittingLogger log = new EmittingLogger(BrokerResource.class);

  @Inject
  public BrokerResource(BrokerServerView brokerServerView, SegmentCollector segmentCollector, DruidBroker druidBroker)
  {
    this.brokerServerView = brokerServerView;
    this.segmentCollector = segmentCollector;
    this.druidBroker = druidBroker;
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus()
  {
    return Response.ok(ImmutableMap.of("inventoryInitialized", brokerServerView.isInitialized())).build();
  }
  
  @GET
  @Path("/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSegments()
  {
    return Response.ok(segmentCollector.getSerializedSegmentList()).build();
  }

  @POST
  @Path("/routingTable")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response applyNewRoutingTable(final byte[] routingTable)
  {
      log.info("Received POST for routing table %s", routingTable.toString());
      Map<String, Map<String, Long>> rt = null;
      try {
          rt = jsonMapper.readValue(
                  //response.getContent(),
                  routingTable,
                  new TypeReference<Map<String, Map<String, Long>>>() {
                  }
          );
      }catch(java.io.IOException e){}

      // save the routing table
      druidBroker.setRoutingTable(rt);
      log.info("Sent response of POST for routing table");
      return Response.ok().build();
  }
}

