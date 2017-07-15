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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import com.metamx.http.client.HttpClient;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.collections.CountingMap;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.query.*;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.QueryManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;

import org.apache.commons.lang3.tuple.Triple;
import org.joda.time.*;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ServerManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);
  private final Object lock = new Object();
  private final SegmentLoader segmentLoader;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final ExecutorService exec;
  private final ExecutorService cachingExec;
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> dataSources;
  private final CountingMap<String> dataSourceSizes = new CountingMap<String>();
  private final CountingMap<String> dataSourceCounts = new CountingMap<String>();
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;
  private final QueryManager manager;
  private final QueryRuntimeEstimator estimator;

  private final String loadingPath = "/proj/DCSQ/mghosh4/druid/estimation/";
  private final String[] fullpaths = {loadingPath+"groupby.cdf", loadingPath+"timeseries.cdf", loadingPath+"topn.cdf"};

  private final HashMap<String, ArrayList<Double>> percentileCollection = new HashMap<String, ArrayList<Double>>();
  private final HashMap<String, HashMap<Double, Double>> histogramCollection = new HashMap<String, HashMap<Double, Double>>();

  private final ConcurrentHashMap<String, MutablePair<Long, Long>> runtimeEstimate = new ConcurrentHashMap<>();
  private long estimatedLoad;
  private long lastLoadEstimateTime;

  private final String[] queryTypes = {Query.TIMESERIES, Query.TOPN, Query.GROUP_BY};

  private final ServerDiscoveryFactory serverDiscoveryFactory;
  private final ScheduledExecutorService pool;
  public static HttpClient httpClient;

  @Inject
  public ServerManager(
      SegmentLoader segmentLoader,
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      @Processing ExecutorService exec,
      @BackgroundCaching ExecutorService cachingExec,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      QueryManager manager,
      QueryRuntimeEstimator estimator,
      ServerDiscoveryFactory serverDiscoveryFactory,
      @Global HttpClient httpClient
  )
  {
    this.segmentLoader = segmentLoader;
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.exec = exec;
    this.cachingExec = cachingExec;
    this.cache = cache;
    this.objectMapper = objectMapper;

    this.dataSources = new HashMap<>();
    this.cacheConfig = cacheConfig;

    this.manager = manager;
    this.estimator = estimator;
    this.estimator.startQueryRuntimeEstimation();

    this.lastLoadEstimateTime = DateTime.now().getMillis();
    this.estimatedLoad = 0;

    //populate all query time distribution data structures
    for(int i = 0 ; i < queryTypes.length; i++){
      String key = queryTypes[i];
      HashMap<Double, Double> histogram = new HashMap<Double, Double>();
      ArrayList<Double> percentile = null;
      try {
        percentile = loadAndParse(fullpaths[i], histogram);
      } catch (IOException e) {
        e.printStackTrace();
        break;
      }
      percentileCollection.put(key, percentile);
      histogramCollection.put(key, histogram);
    }

    this.serverDiscoveryFactory = serverDiscoveryFactory;
    this.httpClient = httpClient;
    log.info("Instantiating periodic load POST task");
    this.pool = Executors.newScheduledThreadPool(1);
    //pool.scheduleWithFixedDelay(new PeriodicLoadUpdate(this, this.serverDiscoveryFactory), 1000, 5, TimeUnit.MILLISECONDS);
  }

  private ArrayList<Double> loadAndParse(String filename, HashMap<Double, Double> histogram) throws IOException {
    ArrayList<Double> percentileArr = new ArrayList<Double>();

    /*********************************************************************/
    /* http://stackoverflow.com/questions/5819772/java-parsing-text-file */
    FileReader input = new FileReader(filename);
    BufferedReader bufRead = new BufferedReader(input);
    String myLine = null;

    while ( (myLine = bufRead.readLine()) != null)
    {
      String[] array = myLine.split("\\s+");
      double querytime = Double.parseDouble(array[0]);
      double percentile = Double.parseDouble(array[1]);
      percentileArr.add(percentile);
      histogram.put(percentile, querytime);
    }

    /*********************************************************************/
    return percentileArr;
  }



  public Map<String, Long> getDataSourceSizes()
  {
    synchronized (dataSourceSizes) {
      return dataSourceSizes.snapshot();
    }
  }

  public Map<String, Long> getDataSourceCounts()
  {
    synchronized (dataSourceCounts) {
      return dataSourceCounts.snapshot();
    }
  }

  public boolean isSegmentCached(final DataSegment segment) throws SegmentLoadingException
  {
    return segmentLoader.isSegmentLoaded(segment);
  }

  /**
   * Load a single segment.
   *
   * @param segment segment to load
   *
   * @return true if the segment was newly loaded, false if it was already loaded
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   */
  public boolean loadSegment(final DataSegment segment) throws SegmentLoadingException
  {
    final Segment adapter;
    try {
      adapter = segmentLoader.getSegment(segment);
    }
    catch (SegmentLoadingException e) {
      try {
        segmentLoader.cleanup(segment);
      }
      catch (SegmentLoadingException e1) {
        // ignore
      }
      throw e;
    }

    if (adapter == null) {
      throw new SegmentLoadingException("Null adapter from loadSpec[%s]", segment.getLoadSpec());
    }

    synchronized (lock) {
      String dataSource = segment.getDataSource();
      VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        loadedIntervals = new VersionedIntervalTimeline<>(Ordering.natural());
        dataSources.put(dataSource, loadedIntervals);
      }

      PartitionHolder<ReferenceCountingSegment> entry = loadedIntervals.findEntry(
          segment.getInterval(),
          segment.getVersion()
      );
      if ((entry != null) && (entry.getChunk(segment.getShardSpec().getPartitionNum()) != null)) {
        log.warn("Told to load a adapter for a segment[%s] that already exists", segment.getIdentifier());
        return false;
      }

      loadedIntervals.add(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk(new ReferenceCountingSegment(adapter))
      );
      synchronized (dataSourceSizes) {
        dataSourceSizes.add(dataSource, segment.getSize());
      }
      synchronized (dataSourceCounts) {
        dataSourceCounts.add(dataSource, 1L);
      }
      return true;
    }
  }

  public void dropSegment(final DataSegment segment) throws SegmentLoadingException
  {
    String dataSource = segment.getDataSource();
    synchronized (lock) {
      VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSource);
        return;
      }

      PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk((ReferenceCountingSegment) null)
      );
      ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();

      if (oldQueryable != null) {
        synchronized (dataSourceSizes) {
          dataSourceSizes.add(dataSource, -segment.getSize());
        }
        synchronized (dataSourceCounts) {
          dataSourceCounts.add(dataSource, -1L);
        }

        try {
          log.info("Attempting to close segment %s", segment.getIdentifier());
          oldQueryable.close();
        }
        catch (IOException e) {
          log.makeAlert(e, "Exception closing segment")
             .addData("dataSource", dataSource)
             .addData("segmentId", segment.getIdentifier())
             .emit();
        }
      } else {
        log.info(
            "Told to delete a queryable on dataSource[%s] for interval[%s] and version [%s] that I don't have.",
            dataSource,
            segment.getInterval(),
            segment.getVersion()
        );
      }
    }
    segmentLoader.cleanup(segment);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
	log.info("======1. Entering getQueryRunnerForIntervals====");
	log.info("(1). get query runner for query [%s] and intervals [%s}", query.getIntervals());
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final Function<Query<T>, ServiceMetricEvent.Builder> builderFn = getBuilderFn(toolChest);
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    DataSource dataSource = query.getDataSource();
    if (!(dataSource instanceof TableDataSource)) {
      throw new UnsupportedOperationException("data source type '" + dataSource.getClass().getName() + "' unsupported");
    }
    String dataSourceName = getDataSourceName(dataSource);

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = dataSources.get(dataSourceName);

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
        .create(intervals)
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>> apply(Interval input)
              {
            	log.info("(1). Lookup interval input [%s] from the timeline", input.toString());
                return timeline.lookup(input);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, ReferenceCountingSegment>, Iterable<QueryRunner<T>>>()
            {
              @Override
              public Iterable<QueryRunner<T>> apply(
                  @Nullable
                  final TimelineObjectHolder<String, ReferenceCountingSegment> holder
              )
              {
                if (holder == null) {
                  return null;
                }

                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<ReferenceCountingSegment>, QueryRunner<T>>()
                        {
                          @Override
                          public QueryRunner<T> apply(PartitionChunk<ReferenceCountingSegment> input)
                          {
                        	log.info("(1). build and decorate Query Runner for [%s] ", holder.getInterval());
                            return buildAndDecorateQueryRunner(
                                factory,
                                toolChest,
                                input.getObject(),
                                new SegmentDescriptor(
                                    holder.getInterval(),
                                    holder.getVersion(),
                                    input.getChunkNumber()
                                ),
                                builderFn,
                                cpuTimeAccumulator
                            );
                          }
                        }
                    );
              }
            }
        );

    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<T>(
            toolChest.mergeResults(factory.mergeRunners(exec, queryRunners)),
            toolChest
        ),
        builderFn,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  public String getConcurrentAccessMap()
  {
	String result = null;

	try {
		Map<String, Integer> concurrentAccessMap = Maps.newHashMap();
		for (Map.Entry<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> dataSource : dataSources.entrySet())
		{
			List<PartitionHolder<ReferenceCountingSegment>> partitionList = dataSource.getValue().getAllObjects();
            for (PartitionHolder<ReferenceCountingSegment> partition : partitionList)
            {
                for (ReferenceCountingSegment segment : partition.payloads())
                    concurrentAccessMap.put(segment.getIdentifier(), segment.getAndClearMaxConcurrentAccess());
            }
		}

		result = objectMapper.writeValueAsString(concurrentAccessMap);
		log.info("Serializing Concurrent Access Map [%d]", result.length());
	} catch (JsonProcessingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	return result;
  }

  public String getTotalAccessMap()
  {
	String result = null;

	try {
		Map<String, Integer> totalAccessMap = Maps.newHashMap();
		for (Map.Entry<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> dataSource : dataSources.entrySet())
		{
			List<PartitionHolder<ReferenceCountingSegment>> partitionList = dataSource.getValue().getAllObjects();
            for (PartitionHolder<ReferenceCountingSegment> partition : partitionList)
            {
                for (ReferenceCountingSegment segment : partition.payloads())
                    totalAccessMap.put(segment.getIdentifier(), segment.getAndClearTotalAccess());
            }
		}

		result = objectMapper.writeValueAsString(totalAccessMap);
		log.info("Serializing Total Access Map [%d]", result.length());
	} catch (JsonProcessingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	return result;
  }

  public String getSegmentAccessTimeMap()
  {
    String result = null;

    try {
      Map<String, Long> segmentAccessTimeMap = Maps.newHashMap();
      for (Map.Entry<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> dataSource : dataSources.entrySet())
      {
        List<PartitionHolder<ReferenceCountingSegment>> partitionList = dataSource.getValue().getAllObjects();
        for (PartitionHolder<ReferenceCountingSegment> partition : partitionList)
        {
          for (ReferenceCountingSegment segment : partition.payloads()) {
            long accessTime = segment.getAndClearSegmentQueryTime();
            if (accessTime > 0)
              segmentAccessTimeMap.put(segment.getIdentifier(), accessTime);
          }
        }
      }

      result = objectMapper.writeValueAsString(segmentAccessTimeMap);
      log.info("Serializing Total Access Map [%d]", result.length());
    } catch (JsonProcessingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return result;
  }

  public String currentQueryLoad()
  {
    String result = null;
    long finalLoadValue = 0;
    MetricsEmittingExecutorService service = (MetricsEmittingExecutorService)(exec);
    finalLoadValue = service.getQueueSize() + service.getActiveTaskCount();
    log.info("Current Load [%d]", finalLoadValue);
    try
    {
        Map<String, Long> returnValue = Maps.newHashMap();
        returnValue.put("currentload", finalLoadValue);
        result = objectMapper.writeValueAsString(returnValue);
    } catch (JsonProcessingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return result;
  }

  public String currentQueueLength()
  {
    long finalLoadValue = 0;
    MetricsEmittingExecutorService service = (MetricsEmittingExecutorService)(exec);
    finalLoadValue = service.getQueueSize() + service.getActiveTaskCount();
    log.info("QSize %d", service.getQueueSize());
    log.info("ActiveTaskCount %d", service.getActiveTaskCount());
    return Long.toString(finalLoadValue);
  }

  public List<Pair<DateTime, Long>> getActiveTaskRuntimeEstimates()
  {
    MetricsEmittingExecutorService service = (MetricsEmittingExecutorService)(exec);
    List<Triple<DateTime, String, Long>> activeRunList = Lists.newArrayList(service.getActiveRunList());

    List<Pair<DateTime, Long>> activeTaskRuntimeEstimates = new ArrayList<>();
    for (Triple<DateTime, String, Long> activeTask : activeRunList)
    {
      activeTaskRuntimeEstimates.add(new Pair<>(activeTask.getLeft(),
              estimator.getQueryRuntimeEstimate(activeTask.getMiddle(), activeTask.getRight())));
    }

    return activeTaskRuntimeEstimates;
  }

  public List<Long> getQueuedTaskRuntimeEstimates()
  {
    MetricsEmittingExecutorService service = (MetricsEmittingExecutorService)(exec);
    List<Pair<String, Long>> queuedTasks = service.getQueuedTasks();

    List<Long> queuedTaskRuntimeEstimates = new ArrayList<>();
    for (Pair<String, Long> task :  queuedTasks)
    {
      queuedTaskRuntimeEstimates.add(estimator.getQueryRuntimeEstimate(task.lhs, task.rhs));
    }

    return queuedTaskRuntimeEstimates;
  }

  public String currentWaitTime()
  {
    MetricsEmittingExecutorService service = (MetricsEmittingExecutorService)(exec);
    if (service.getActiveTaskCount() < service.getCorePoolSize())
      return Long.toString(0);

    //TODO: Run only if a new task has been inserted
    if (!service.isNewTaskAdded())
    {
        long currentTimeInMillis = DateTime.now().getMillis();
        long timespent = currentTimeInMillis - lastLoadEstimateTime;
        estimatedLoad = estimatedLoad - timespent;
        if (estimatedLoad < 0)
            estimatedLoad = 0;
        lastLoadEstimateTime = currentTimeInMillis;

        return Long.toString(estimatedLoad);
    }

    List<Pair<DateTime, Long>> activeRunList = getActiveTaskRuntimeEstimates();
    List<Long> queuedTasks = getQueuedTaskRuntimeEstimates();
    DateTime simTime = DateTime.now();

    while (!queuedTasks.isEmpty())
    {
      Pair<DateTime, Long> taskToFinish = Collections.min(activeRunList, new Comparator<Pair<DateTime, Long>>() {
        @Override
        public int compare(Pair<DateTime, Long> o1, Pair<DateTime, Long> o2) {
          return o1.lhs.withDurationAdded(o1.rhs, 1).compareTo(
                  o2.lhs.withDurationAdded(o2.rhs, 1));
        }
      });

      simTime = taskToFinish.lhs.withDurationAdded(taskToFinish.rhs, 1);
      activeRunList.remove(taskToFinish);
      activeRunList.add(new Pair<>(simTime, queuedTasks.get(0)));
      queuedTasks.remove(0);
    }

    Pair<DateTime, Long> taskToFinish = Collections.min(activeRunList, new Comparator<Pair<DateTime, Long>>() {
      @Override
      public int compare(Pair<DateTime, Long> o1, Pair<DateTime, Long> o2) {
        return o1.lhs.withDurationAdded(o1.rhs, 1).compareTo(
                o2.lhs.withDurationAdded(o2.rhs, 1));
      }
    });
    simTime = taskToFinish.lhs.withDurationAdded(taskToFinish.rhs, 1);

    lastLoadEstimateTime = DateTime.now().getMillis();
    estimatedLoad = simTime.getMillis();
    return Long.toString(estimatedLoad);
  }

  private String getDataSourceName(DataSource dataSource)
  {
    return Iterables.getOnlyElement(dataSource.getNames());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
	log.info("======6. getQueryRunner For Segments [%s]====", query.toString());
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<T>();
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    String dataSourceName = getDataSourceName(query.getDataSource());

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = dataSources.get(
        dataSourceName
    );

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    final Function<Query<T>, ServiceMetricEvent.Builder> builderFn = getBuilderFn(toolChest);
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
        .create(specs)
        .transformCat(
            new Function<SegmentDescriptor, Iterable<QueryRunner<T>>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Iterable<QueryRunner<T>> apply(SegmentDescriptor input)
              {

            	log.info("(6). input segment descriptor [%s]", input.getInterval());
                final PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
                    input.getInterval(), input.getVersion()
                );

                if (entry == null) {
                  return Arrays.<QueryRunner<T>>asList(new ReportTimelineMissingSegmentQueryRunner<T>(input));
                }

                final PartitionChunk<ReferenceCountingSegment> chunk = entry.getChunk(input.getPartitionNumber());
                if (chunk == null) {
                  return Arrays.<QueryRunner<T>>asList(new ReportTimelineMissingSegmentQueryRunner<T>(input));
                }

                final ReferenceCountingSegment adapter = chunk.getObject();
                final QueryRunner<T> runner = buildAndDecorateQueryRunner(factory, toolChest, adapter, input, builderFn, cpuTimeAccumulator);
                runner.durationMap.put(runner, input.getInterval().toDurationMillis());
                return Arrays.asList(runner);
              }
            }
        );

    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(exec, queryRunners)),
            toolChest
        ),
        builderFn,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final ReferenceCountingSegment adapter,
      final SegmentDescriptor segmentDescriptor,
      final Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      final AtomicLong cpuTimeAccumulator
  )
  {
	log.info("======3. buildAndDecorateQueryRunner====");
    SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
    return CPUTimeMetricQueryRunner.safeBuild(
        new SpecificSegmentQueryRunner<T>(
            new MetricsEmittingQueryRunner<T>(
                emitter,
                builderFn,
                new BySegmentQueryRunner<T>(
                    adapter.getIdentifier(),
                    adapter.getDataInterval().getStart(),
                    new CachingQueryRunner<T>(
                        adapter.getIdentifier(),
                        segmentDescriptor,
                        objectMapper,
                        cache,
                        toolChest,
                        new MetricsEmittingQueryRunner<T>(
                            emitter,
                            new Function<Query<T>, ServiceMetricEvent.Builder>()
                            {
                              @Override
                              public ServiceMetricEvent.Builder apply(@Nullable final Query<T> input)
                              {
                                return toolChest.makeMetricBuilder(input);
                              }
                            },
                            new ReferenceCountingSegmentQueryRunner<T>(factory, adapter, segmentDescriptor),
                            "query/segment/time",
                            ImmutableMap.of("segment", adapter.getIdentifier()),
                            adapter,
                            estimator
                        ),
                        cachingExec,
                        cacheConfig
                    )
                ),
                "query/segmentAndCache/time",
                ImmutableMap.of("segment", adapter.getIdentifier()),
                adapter,
                estimator
            ).withWaitMeasuredFromNow(),
            segmentSpec
        ),
        builderFn,
        emitter,
        cpuTimeAccumulator,
        false
    );
  }

  private static <T> Function<Query<T>, ServiceMetricEvent.Builder> getBuilderFn(final QueryToolChest<T, Query<T>> toolChest)
  {
    return new Function<Query<T>, ServiceMetricEvent.Builder>()
    {
      @Nullable
      @Override
      public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
      {
        return toolChest.makeMetricBuilder(input);
      }
    };
  }
}
