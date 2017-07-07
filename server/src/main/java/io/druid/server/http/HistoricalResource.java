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

import com.google.common.collect.ImmutableMap;
import io.druid.server.coordination.ZkCoordinator;
import io.druid.server.coordination.ServerManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/historical/v1")
public class HistoricalResource
{
  private final ZkCoordinator coordinator;
  private final ServerManager manager;

  @Inject
  public HistoricalResource(
      ZkCoordinator coordinator,
      ServerManager manager
  )
  {
    this.coordinator = coordinator;
    this.manager = manager;
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus()
  {
    return Response.ok(ImmutableMap.of("cacheInitialized", coordinator.isStarted())).build();
  }

  @GET
  @Path("/concurrentAccess")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConcurrentAccess()
  {
    return Response.ok(manager.getConcurrentAccessMap()).build();
  }

  @GET
  @Path("/totalAccess")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTotalAccess()
  {
    return Response.ok(manager.getTotalAccessMap()).build();
  }

  @GET
  @Path("/totalAccessTime")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTotalAccessTime()
  {
    return Response.ok(manager.getSegmentAccessTimeMap()).build();
  }

  @GET
  @Path("/currentLoad")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCurrentQueryLoad()
  {
    return Response.ok(manager.currentWaitTime()).build();
  }
}
