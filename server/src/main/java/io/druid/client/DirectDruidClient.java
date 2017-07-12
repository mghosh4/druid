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

package io.druid.client;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.RE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.server.coordination.broker.DruidBroker;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(DirectDruidClient.class);

  private static final Map<Class<? extends Query>, Pair<JavaType, JavaType>> typesMap = Maps.newConcurrentMap();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String host;
  private final ServiceEmitter emitter;
  private final DruidServer server;
  private final DruidBroker druidBroker;
  private final AtomicInteger openConnections;
  private final boolean isSmile;

  public DirectDruidClient(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      String host,
      ServiceEmitter emitter,
      DruidServer server,
      DruidBroker druidBroker
  )
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.host = host;
    this.emitter = emitter;
    this.server = server;
    this.druidBroker = druidBroker;

    this.isSmile = this.objectMapper.getFactory() instanceof SmileFactory;
    this.openConnections = new AtomicInteger();
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> context)
  {
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    boolean isBySegment = BaseQuery.getContextBySegment(query, false);

    Pair<JavaType, JavaType> types = typesMap.get(query.getClass());
    if (types == null) {
      final TypeFactory typeFactory = objectMapper.getTypeFactory();
      JavaType baseType = typeFactory.constructType(toolChest.getResultTypeReference());
      JavaType bySegmentType = typeFactory.constructParametricType(
          Result.class, typeFactory.constructParametricType(BySegmentResultValueClass.class, baseType)
      );
      types = Pair.of(baseType, bySegmentType);
      typesMap.put(query.getClass(), types);
    }

    final JavaType typeRef;
    if (isBySegment) {
      typeRef = types.rhs;
    } else {
      typeRef = types.lhs;
    }

    final ListenableFuture<InputStream> future;
    final String url = String.format("http://%s/druid/v2/", host);
    final String cancelUrl = String.format("http://%s/druid/v2/%s", host, query.getId());

    try {
      log.debug("Querying url[%s]", url);

      final long requestStartTime = System.currentTimeMillis();

      final ServiceMetricEvent.Builder builder = toolChest.makeMetricBuilder(query);
      builder.setDimension("server", host);
      builder.setDimension(DruidMetrics.ID, Strings.nullToEmpty(query.getId()));

      final HttpResponseHandler<InputStream, InputStream> responseHandler = new HttpResponseHandler<InputStream, InputStream>()
      {
        private long responseStartTime;
        private final AtomicLong byteCount = new AtomicLong(0);
        private final BlockingQueue<InputStream> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean done = new AtomicBoolean(false);

        @Override
        public ClientResponse<InputStream> handleResponse(HttpResponse response)
        {
          log.debug("Initial response from url[%s]", url);
          responseStartTime = System.currentTimeMillis();
          final String queryType = query.getType();
          final long queryDurationMillis = query.getDuration().getMillis();
          emitter.emit(builder.build("query/node/ttfb", responseStartTime - requestStartTime));

          try {
            try {
              SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
              final String currentHNLoad = "0";//response.headers().get("CurrentHNLoad");
              final String currentHNLoadRuntime = response.headers().get("CurrentHNLoadRuntime");
              final Date currentHNLoadTime = sdf.parse(response.headers().get("CurrentHNLoadTime"));
              final long hnQueryTimeMillis = 0;//Long.valueOf(response.headers().get("HNQueryTime"));
              String hnQuerySegmentTimeStr = response.headers().get("HNQuerySegmentTime");

              String[] hnQuerySegmentTimes = hnQuerySegmentTimeStr.split(",");
              long hnQuerySegmentTimeMillis = 0L;
              if(hnQuerySegmentTimeStr != ""){
                hnQuerySegmentTimeMillis = Long.valueOf(hnQuerySegmentTimeStr);
              }

              // update queryRuntimeEstimateTable
              //druidBroker.setQueryRuntimeEstimate(queryType, queryDurationMillis, hnQueryTimeMillis);
              if(!hnQuerySegmentTimeStr.equals("")) {
                for (int i = 0; i < hnQuerySegmentTimes.length; i = i + 2) {
                  druidBroker.setQueryRuntimeEstimate(queryType, Long.valueOf(hnQuerySegmentTimes[i]), Long.valueOf(hnQuerySegmentTimes[i + 1]));
                  //log.info("Setting decayed estimate queryId %s, queryType %s, duration %d, query segment time %d", query.getId(), query.getType(), Long.valueOf(hnQuerySegmentTimes[i]), Long.valueOf(hnQuerySegmentTimes[i + 1]));
                }
              }
              else{
                log.info("Received empty query segment time queryID %s queryType %s hnQuerySegmentTimeStr %s", query.getId(), query.getType(), hnQuerySegmentTimeStr);
              }

              log.info("Stats queryId %s, queryType %s, query duration %d, query node time %d, query time %d, query segment time %s, hn load %s, hn load runtime %s", query.getId(), queryType, queryDurationMillis, (System.currentTimeMillis()-requestStartTime), hnQueryTimeMillis, hnQuerySegmentTimeStr, currentHNLoad, currentHNLoadRuntime);

              /*
              // calculate the exponential moving average of load over n data samples
              long prevLoad = server.getCurrentLoad();
              if(prevLoad == -1){
                // first load sample
                server.setCurrentLoad(Long.parseLong(currentHNLoad));
              }
              else{
                int numSamples = 3;
                float alpha = 2/(1+numSamples);
                long currLoad = (long)(alpha*Long.parseLong(currentHNLoad) + (1-alpha)*prevLoad);
                server.setCurrentLoad(currLoad);
              }
              */

              // check if current update is latest
              //log.info("Server load update new load=%s, newHNLoadTime=%s, prev load=%d, prevHNLoadTime=%s", currentHNLoad, sdf.format(currentHNLoadTime), server.getCurrentLoad(), sdf.format(server.getCurrentLoadTimeAtServer()));
              if(currentHNLoadTime.before(server.getCurrentLoadTimeAtServer())) {
                log.info("Out of order server load updates prev=%s, new=%s", sdf.format(server.getCurrentLoadTimeAtServer()), sdf.format(currentHNLoadTime));
              }
              else{
                //server.setCurrentLoad(Long.parseLong(currentHNLoad));
                server.setCurrentLoad(Long.parseLong((currentHNLoadRuntime)));
                server.setCurrentLoadTimeAtServer(currentHNLoadTime);
              }
              //log.info("Current HN [%s] load [%s]", host, currentHNLoad);
            }catch(java.text.ParseException e){}
            
            final String responseContext = response.headers().get("X-Druid-Response-Context");
            // context may be null in case of error or query timeout
            if (responseContext != null) {
              context.putAll(
                  objectMapper.<Map<String, Object>>readValue(
                      responseContext, new TypeReference<Map<String, Object>>()
                      {
                      }
                  )
              );
            }
            queue.put(new ChannelBufferInputStream(response.getContent()));
          }
          catch (final IOException e) {
            log.error(e, "Error parsing response context from url [%s]", url);
            return ClientResponse.<InputStream>finished(
                new InputStream()
                {
                  @Override
                  public int read() throws IOException
                  {
                    throw e;
                  }
                }
            );
          }
          catch (InterruptedException e) {
            log.error(e, "Queue appending interrupted");
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
          }
          byteCount.addAndGet(response.getContent().readableBytes());
          return ClientResponse.<InputStream>finished(
              new SequenceInputStream(
                  new Enumeration<InputStream>()
                  {
                    @Override
                    public boolean hasMoreElements()
                    {
                      // Done is always true until the last stream has be put in the queue.
                      // Then the stream should be spouting good InputStreams.
                      synchronized (done) {
                        return !done.get() || !queue.isEmpty();
                      }
                    }

                    @Override
                    public InputStream nextElement()
                    {
                      try {
                        return queue.take();
                      }
                      catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                      }
                    }
                  }
              )
          );
        }

        @Override
        public ClientResponse<InputStream> handleChunk(
            ClientResponse<InputStream> clientResponse, HttpChunk chunk
        )
        {
          final ChannelBuffer channelBuffer = chunk.getContent();
          final int bytes = channelBuffer.readableBytes();
          if (bytes > 0) {
            try {
              queue.put(new ChannelBufferInputStream(channelBuffer));
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            byteCount.addAndGet(bytes);
          }
          return clientResponse;
        }

        @Override
        public ClientResponse<InputStream> done(ClientResponse<InputStream> clientResponse)
        {
          long stopTime = System.currentTimeMillis();
          log.debug(
              "Completed request to url[%s] with %,d bytes returned in %,d millis [%,f b/s].",
              url,
              byteCount.get(),
              stopTime - responseStartTime,
              byteCount.get() / (0.0001 * (stopTime - responseStartTime))
          );
          emitter.emit(builder.build("query/node/time", stopTime - requestStartTime));
          emitter.emit(builder.build("query/node/bytes", byteCount.get()));
          synchronized (done) {
            try {
              // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
              // after done is set to true, regardless of the rest of the stream's state.
              queue.put(ByteSource.empty().openStream());
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            catch (IOException e) {
              // This should never happen
              throw Throwables.propagate(e);
            }
            finally {
              done.set(true);
            }
          }
          return ClientResponse.<InputStream>finished(clientResponse.getObj());
        }

        @Override
        public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e)
        {
          // Don't wait for lock in case the lock had something to do with the error
          synchronized (done) {
            done.set(true);
            // Make a best effort to put a zero length buffer into the queue in case something is waiting on the take()
            // If nothing is waiting on take(), this will be closed out anyways.
            queue.offer(
                new InputStream()
                {
                  @Override
                  public int read() throws IOException
                  {
                    throw new IOException(e);
                  }
                }
            );
          }
        }
      };

      long queryRuntimeEstimate = druidBroker.getQueryRuntimeEstimate(query.getType(), query.getDuration().getMillis());
      log.info("Query details type %s, intervals %s, duration millis %d, datasource %s, runtime estimate %d", query.getType(), query.getIntervals().toString(), query.getDuration().getMillis(), query.getDataSource().getNames().toString(), queryRuntimeEstimate);

      future = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(url)
          ).setContent(objectMapper.writeValueAsBytes(query))
           .setHeader(
               HttpHeaders.Names.CONTENT_TYPE,
               isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE+","+String.valueOf(queryRuntimeEstimate) :
               MediaType.APPLICATION_JSON+","+String.valueOf(queryRuntimeEstimate)
           ),
           //.setHeader(HttpHeaders.Names.COOKIE, String.valueOf(queryRuntimeEstimate)),
          responseHandler
      );

      queryWatcher.registerQuery(query, future);

      openConnections.getAndIncrement();
      Futures.addCallback(
          future, new FutureCallback<InputStream>()
          {
            @Override
            public void onSuccess(InputStream result)
            {
              openConnections.getAndDecrement();
            }

            @Override
            public void onFailure(Throwable t)
            {
              openConnections.getAndDecrement();
              if (future.isCancelled()) {
                // forward the cancellation to underlying queriable node
                try {
                  StatusResponseHolder res = httpClient.go(
                      new Request(
                          HttpMethod.DELETE,
                          new URL(cancelUrl)
                      ).setContent(objectMapper.writeValueAsBytes(query))
                       .setHeader(
                           HttpHeaders.Names.CONTENT_TYPE,
                           isSmile
                           ? SmileMediaTypes.APPLICATION_JACKSON_SMILE
                           : MediaType.APPLICATION_JSON
                       ),
                      new StatusResponseHandler(Charsets.UTF_8)
                  ).get();
                  if (res.getStatus().getCode() >= 500) {
                    throw new RE(
                        "Error cancelling query[%s]: queriable node returned status[%d] [%s].",
                        res.getStatus().getCode(),
                        res.getStatus().getReasonPhrase()
                    );
                  }
                }
                catch (IOException | ExecutionException | InterruptedException e) {
                  Throwables.propagate(e);
                }
              }
            }
          }
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    Sequence<T> retVal = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            return new JsonParserIterator<T>(typeRef, future, url);
          }

          @Override
          public void cleanup(JsonParserIterator<T> iterFromMake)
          {
            CloseQuietly.close(iterFromMake);
          }
        }
    );

    // bySegment queries are de-serialized after caching results in order to
    // avoid the cost of de-serializing and then re-serializing again when adding to cache
    if (!isBySegment) {
      retVal = Sequences.map(
          retVal,
          toolChest.makePreComputeManipulatorFn(
              query,
              MetricManipulatorFns.deserializing()
          )
      );
    }

    return retVal;
  }

  private class JsonParserIterator<T> implements Iterator<T>, Closeable
  {
    private JsonParser jp;
    private ObjectCodec objectCodec;
    private final JavaType typeRef;
    private final Future<InputStream> future;
    private final String url;

    public JsonParserIterator(JavaType typeRef, Future<InputStream> future, String url)
    {
      this.typeRef = typeRef;
      this.future = future;
      this.url = url;
      jp = null;
    }

    @Override
    public boolean hasNext()
    {
      init();

      if (jp.isClosed()) {
        return false;
      }
      if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
        CloseQuietly.close(jp);
        return false;
      }

      return true;
    }

    @Override
    public T next()
    {
      init();
      try {
        final T retVal = objectCodec.readValue(jp, typeRef);
        jp.nextToken();
        return retVal;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    private void init()
    {
      if (jp == null) {
        try {
          jp = objectMapper.getFactory().createParser(future.get());
          final JsonToken nextToken = jp.nextToken();
          if (nextToken == JsonToken.START_OBJECT) {
            QueryInterruptedException e = jp.getCodec().readValue(jp, QueryInterruptedException.class);
            throw e;
          } else if (nextToken != JsonToken.START_ARRAY) {
            throw new IAE("Next token wasn't a START_ARRAY, was[%s] from url [%s]", jp.getCurrentToken(), url);
          } else {
            jp.nextToken();
            objectCodec = jp.getCodec();
          }
        }
        catch (IOException | InterruptedException | ExecutionException e) {
          throw new RE(e, "Failure getting results from[%s] because of [%s]", url, e.getMessage());
        }
        catch (CancellationException e) {
          throw new QueryInterruptedException("Query cancelled");
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      if (jp != null) {
        jp.close();
      }
    }
  }
}
