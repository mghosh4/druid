package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.emitter.EmittingLogger;
import io.druid.query.MutablePair;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.*;
import io.druid.timeline.DataSegment;

import javax.xml.crypto.Data;
import java.util.*;


public class DruidCoordinatorReplicatorHelper {

    private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorReplicatorHelper.class);
    private static final String assignedCount = "assignedCount";
    private static final String droppedCount = "droppedCount";

    public static CoordinatorStats assign(
            final ReplicationThrottler replicationManager,
            final String tier,
            final ServerHolder holder,
            final DataSegment segment)
    {
        log.debug("Insert Segment [%s] to [%s]", segment.getIdentifier(), holder.getServer().getHost());

        final CoordinatorStats stats = new CoordinatorStats();
        stats.addToTieredStat(assignedCount, tier, 0);

        replicationManager.registerReplicantCreation(
                tier, segment.getIdentifier(), holder.getServer().getHost()
        );

        holder.getPeon().loadSegment(
                segment,
                new LoadPeonCallback()
                {
                    @Override
                    public void execute()
                    {
                        replicationManager.unregisterReplicantCreation(
                                tier,
                                segment.getIdentifier(),
                                holder.getServer().getHost()
                        );
                    }
                }
        );

        stats.addToTieredStat(assignedCount, tier, 1);

        return stats;
    }

    public static CoordinatorStats drop(
            final ReplicationThrottler replicationManager,
            final String tier,
            final ServerHolder holder,
            final DataSegment segment)
    {
        log.debug("Remove Segment [%s] from [%s]", segment.getIdentifier(), holder.getServer().getHost());
        CoordinatorStats stats = new CoordinatorStats();

        //log.debug("Removing Segment from [%s] because it has [%d] segments", entry.getValue().getServer().getMetadata().toString(), entry.getKey().intValue());
        stats.addToTieredStat(droppedCount, tier, 0);

        if (holder.isServingSegment(segment)) {
            replicationManager.registerReplicantTermination(
                    tier,
                    segment.getIdentifier(),
                    holder.getServer().getHost()
            );
        }

        holder.getPeon().dropSegment(
                segment,
                new LoadPeonCallback()
                {
                    @Override
                    public void execute()
                    {
                        replicationManager.unregisterReplicantTermination(
                                tier,
                                segment.getIdentifier(),
                                holder.getServer().getHost()
                        );
                    }
                }
        );

        stats.addToTieredStat(droppedCount, tier, 1);

        return stats;
    }

    public static void loadNewSegments(
            DruidCoordinatorRuntimeParams params,
            final HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable,
            DruidCoordinator coordinator)
    {
        List<ServerHolder> serverHolderList = new ArrayList<ServerHolder>();
        for (MinMaxPriorityQueue<ServerHolder> serverQueue : params.getDruidCluster().getSortedServersByTier()) {
            serverHolderList.addAll(serverQueue);
        }

        if (serverHolderList.size() == 0) {
            log.makeAlert("Cluster has no servers! Check your cluster configuration!").emit();
            return;
        }

        final List<String> tierNameList = Lists.newArrayList(params.getDruidCluster().getTierNames());
        if (tierNameList.size() == 0) {
            log.makeAlert("Cluster has multiple tiers! Check your cluster configuration!").emit();
            return;
        }
        final String tier = tierNameList.get(0);

        Set<DataSegment> orderedAvailableDataSegments = coordinator.getOrderedAvailableDataSegments();
        log.debug("latest segment: " + coordinator.getLatestSegment());

        int holderCount = 0;
        for (DataSegment segment : orderedAvailableDataSegments) {
            //log.debug("segment:" + segment.getIdentifier());
            if (segment.getIdentifier().equals(coordinator.getLatestSegment())) {
                break;
            }

            //Round robin allocate the servers because best fit should have uniformly spread the query load
            long bootstrapReplicas = numOfBootstrapReplicasToCreate();
            HashMap<DruidServerMetadata, Long> bootstrapRouting = new HashMap<>();
            for (long replicaNum = 0; replicaNum < bootstrapReplicas; replicaNum++)
            {
                bootstrapRouting.put(serverHolderList.get(holderCount).getServer().getMetadata(), 1L);
                holderCount = (holderCount + 1) % serverHolderList.size();
            }

            routingTable.put(segment, bootstrapRouting);
        }

        if (!orderedAvailableDataSegments.isEmpty()) {
            //log.debug("set latest segment:" + orderedAvailableDataSegments.iterator().next().getIdentifier());
            coordinator.setLatestSegment(orderedAvailableDataSegments.iterator().next().getIdentifier());
        }
    }

    private static long numOfBootstrapReplicasToCreate()
    {
        return 1L;
    }

    // 1.1
    public static HashMap<DruidServerMetadata,Integer> metadataToID(HashMap<DruidServerMetadata, Long> nodeCapacities) {
        HashMap<DruidServerMetadata, Integer> ret  = new HashMap<DruidServerMetadata, Integer>();
        int count = 0;
        for(Map.Entry<DruidServerMetadata, Long> entry : nodeCapacities.entrySet()){
           if(!ret.containsKey(entry.getKey())){
                ret.put(entry.getKey(), count);
                count++;
           }
        }
        return ret;
    }

    // 1.2
    public static HashMap<Integer,DruidServerMetadata> IDToMetadata(HashMap<DruidServerMetadata, Integer> metadataToIDMap) {

        HashMap<Integer,DruidServerMetadata> ret = new HashMap<Integer, DruidServerMetadata>();
        for(Map.Entry<DruidServerMetadata, Integer> entry : metadataToIDMap.entrySet()){
            if(!ret.containsKey(entry.getValue())){
                ret.put(entry.getValue(), entry.getKey());
            }
            else{
                log.debug("IDToMetaData: ID [%s] contains multiple server map to it", entry.getValue());
            }
        }
        for(Map.Entry<Integer,DruidServerMetadata> e : ret.entrySet()){
            log.debug("IDToMetaData: ID [%s] maps to metadata [%s]", e.getKey(), e.getValue().getHost());
        }
        return ret;
    }

    // 2, 3
    public static HashMap< Integer, HashMap<DataSegment, Integer>>  constructReverseMap(
            HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable,
            HashMap<DruidServerMetadata, Integer> metadataToIDMap){

        HashMap<Integer, HashMap<DataSegment, Integer>> ret = new HashMap<Integer, HashMap<DataSegment, Integer>>();
        for(Map.Entry<DataSegment, HashMap<DruidServerMetadata,Long>> entry : routingTable.entrySet()){
            for(Map.Entry<DruidServerMetadata, Long> e : entry.getValue().entrySet()){
                int id = metadataToIDMap.get(e.getKey());
                if(!ret.containsKey(id)){
                    //id doesnt exist
                    HashMap<DataSegment, Integer> map = new HashMap<DataSegment, Integer>();
                    //if id doesnt exist, then the value map doesn exit either, assume nothing in the map
                    map.put(entry.getKey(), 1);
                    ret.put(id, map);
                }
                else{
                    //id exists
                    if(!ret.get(id).containsKey(entry.getKey())){
                        //segment doesnt exists
                        HashMap<DataSegment, Integer> map = new HashMap<DataSegment, Integer>();
                        ret.get(id).put(entry.getKey(), 1);
                    }
                    else{
                        //segment also exists
                        int orig_count = ret.get(id).get(entry.getKey());
                        orig_count++;
                        ret.get(id).put(entry.getKey(), orig_count);
                    }

                }
            }
        }

        /*log.debug("pringReverseMap:");
        for(Map.Entry<Integer, HashMap<DataSegment, Integer>> e : ret.entrySet()){
            log.debug("server: [%s] segmentList: ", e.getKey());
            for(Map.Entry<DataSegment, Integer> entry : e.getValue().entrySet()){
                log.debug("[%s]: [%s]", entry.getKey().getIdentifier(), entry.getValue());
            }
        }*/

        return ret;
    }


    // 4
    public static double[][] buildCostMatrix (HashMap< Integer, HashMap<DataSegment, Integer>> before,
                                           HashMap< Integer, HashMap<DataSegment, Integer>> after){

        if(before.size()!=after.size()){
            log.debug("buildCostMatrix: before and after map have different sizes");
        }

        //printRoutingMaps(before, "BEFORE MAP:");
        //printRoutingMaps(after, "AFTER MAP:");
        double[][] ret = new double[before.size()][after.size()];

        for(Map.Entry<Integer, HashMap<DataSegment, Integer>> entryA : before.entrySet()){
            for(Map.Entry<Integer, HashMap<DataSegment,Integer>> entryB : after.entrySet()){
                int row = entryA.getKey();
                int col = entryB.getKey();
                double cost = 0.0;

                for(Map.Entry<DataSegment, Integer> e : entryB.getValue().entrySet()){
                    if(!entryA.getValue().containsKey(e.getKey())){
                        cost = cost + (double)e.getValue();
                    }
                }
                ret[row][col] = cost;
            }
        }

        /*log.debug("------ buildCostMatrix: printing matrix -------");
        for(int i = 0; i<before.size();i++){
            log.debug("row %s: %s", i, Arrays.toString(ret[i]));
        }*/

        return ret;
    }

    private static void printRoutingMaps(HashMap<Integer, HashMap<DataSegment, Integer>> map, String arg) {
        log.debug("pringRoutingMap: [%s]", arg);
        for(Map.Entry<Integer, HashMap<DataSegment, Integer>> e : map.entrySet()){
            log.debug("server: [%s] segmentList: ", e.getKey());
            for(Map.Entry<DataSegment, Integer> entry : e.getValue().entrySet()){
                log.debug("[%s]: [%s]", entry.getKey().getIdentifier(), entry.getValue());
            }
        }
    }

    public static void printRoutingTable(final HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable){
        int numSegments = 0;
        int numSegmentReplicas = 0;
        for(Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet()){
            log.debug("Segment [%s]:", entry.getKey().getIdentifier());
            numSegments++;
            for(Map.Entry<DruidServerMetadata, Long> e: entry.getValue().entrySet()){
                log.debug("HN [%s]: [%s]", e.getKey().getHost(), e.getValue());
                numSegmentReplicas++;
            }
        }
        log.debug("Replication factor=%f, num segments=%d, num replicas=%d", (float)numSegmentReplicas/(float)numSegments, numSegments, numSegmentReplicas);
    }

    public static int[] hungarianMatching(double[][] m){
        HungarianAlgorithm h = new HungarianAlgorithm(m);
        return h.execute();
    }


    public static HashMap<DataSegment,HashMap<DruidServerMetadata,Long>> rebuildRouting(HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable, int[] hungarianMap, HashMap<DruidServerMetadata, Integer> metadataToIDMap, HashMap<Integer, DruidServerMetadata> IDToMetadataMap) {
        HashMap<DataSegment, HashMap<DruidServerMetadata,Long>> ret = new HashMap<DataSegment, HashMap<DruidServerMetadata, Long>>();
        for(Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet()){
            ret.put(entry.getKey(), new HashMap<DruidServerMetadata, Long>());
            for(Map.Entry<DruidServerMetadata, Long> e : entry.getValue().entrySet()){
                int id = metadataToIDMap.get(e.getKey());
                int mappedID = hungarianMap[id];
                DruidServerMetadata mappedServerMetadata = IDToMetadataMap.get(mappedID);
                ret.get(entry.getKey()).put(mappedServerMetadata, e.getValue());
            }
        }
        return ret;

    }

    private static final Comparator<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> allocCompAscending =
            new Comparator<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>>() {
        @Override
        public int compare(MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> left, MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> right) {
            return Longs.compare(left.rhs, right.rhs);
        }
    };

    private static final Comparator<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> allocCompDescending =
            new Comparator<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>>() {
        @Override
        public int compare(MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> left, MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> right) {
            return Longs.compare(left.rhs, right.rhs)*(-1);
        }
    };

    private static int buildHnSegmentMaps(HashMap<DruidServerMetadata, Integer> hnSegmentCountMap,
                                       HashMap<DruidServerMetadata, Long> hnAllocMap,
                                       HashMap<DruidServerMetadata, HashMap<DataSegment,Long>> hnToSegMap,
                                       HashMap<DataSegment, HashMap<DruidServerMetadata,Long>> routingTable){
        int numSegmentReplicas = 0;
        int numHns = 0;

        for(Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> e1 : routingTable.entrySet()) {
            for (Map.Entry<DruidServerMetadata, Long> e2 : e1.getValue().entrySet()) {
                HashMap<DataSegment, Long> temp = null;
                if (hnToSegMap.get(e2.getKey()) == null) {
                    temp = new HashMap<>();
                    numHns++;
                } else {
                    temp = hnToSegMap.get(e2.getKey());
                }
                temp.put(e1.getKey(), e2.getValue());
                hnToSegMap.put(e2.getKey(), temp);

                if (hnSegmentCountMap.get(e2.getKey()) == null) {
                    hnSegmentCountMap.put(e2.getKey(), 1);
                } else {
                    Integer numSegments = hnSegmentCountMap.get(e2.getKey());
                    numSegments++;
                    hnSegmentCountMap.put(e2.getKey(), numSegments);
                }
                numSegmentReplicas++;

                if (hnAllocMap.get(e2.getKey()) == null) {
                    hnAllocMap.put(e2.getKey(), e2.getValue());
                } else {
                    Long alloc = hnAllocMap.get(e2.getKey());
                    alloc += e2.getValue();
                    hnAllocMap.put(e2.getKey(), alloc);
                }
            }
        }
        return numSegmentReplicas;
    }

//    private static void createUnderOverLists(List<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> under,
//                                             List<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> over,
//                                             HashMap<DruidServerMetadata, Integer> hnSegmentCountMap,
//                                             HashMap<DruidServerMetadata, HashMap<DataSegment,Long>> hnToSegMap,
//                                             int numSegmentsPerHnGoal){
//        for(Map.Entry<DruidServerMetadata, Integer> e1 : hnSegmentCountMap.entrySet()){
//            if (e1.getValue() < numSegmentsPerHnGoal){
//                // get the full allocation of this hn
//                Long allocation = 0L;
//                for(Map.Entry<DataSegment, Long> e2 : hnToSegMap.get(e1.getKey()).entrySet()) {
//                    allocation += e2.getValue();
//                }
//
//                MutablePair<DataSegment, DruidServerMetadata> p1 = new MutablePair<>(null, e1.getKey());
//                MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> p2 = new MutablePair<>(p1, allocation);
//                under.add(p2);
//
////                for(Map.Entry<DataSegment, Long> e2 : hnToSegMap.get(e1.getKey()).entrySet()) {
////                    MutablePair<DataSegment, DruidServerMetadata> p1 = new MutablePair<>(e2.getKey(), e1.getKey());
////                    MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> p2 = new MutablePair<>(p1, e2.getValue());
////                    under.add(p2);
////                }
//            }
//            else if(e1.getValue() > numSegmentsPerHnGoal){
//                List<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> temp =
//                        new ArrayList<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>>();
//                for(Map.Entry<DataSegment, Long> e2 : hnToSegMap.get(e1.getKey()).entrySet()) {
//                    MutablePair<DataSegment, DruidServerMetadata> p1 = new MutablePair<>(e2.getKey(), e1.getKey());
//                    MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> p2 = new MutablePair<>(p1, e2.getValue());
//                    temp.add(p2);
//                }
//                Collections.sort(temp, allocCompAscending);
//                for(int i=0; i<(temp.size()-numSegmentsPerHnGoal)-1; i++){
//                    over.add(temp.get(i));
//                }
//            }
//        }
//    }

    private static void createUnderOverLists(List<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> under,
                                             List<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> over,
                                             HashMap<DruidServerMetadata, Integer> hnSegmentCountMap,
                                             HashMap<DruidServerMetadata, HashMap<DataSegment,Long>> hnToSegMap,
                                             int numSegmentsPerHnGoal){

        for(Map.Entry<DruidServerMetadata, Integer> e1 : hnSegmentCountMap.entrySet()){
            if(e1.getValue() > numSegmentsPerHnGoal){
                List<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>> temp =
                        new ArrayList<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long>>();
                for(Map.Entry<DataSegment, Long> e2 : hnToSegMap.get(e1.getKey()).entrySet()) {
                    MutablePair<DataSegment, DruidServerMetadata> p1 = new MutablePair<>(e2.getKey(), e1.getKey());
                    MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> p2 = new MutablePair<>(p1, e2.getValue());
                    temp.add(p2);
                }
                Collections.sort(temp, allocCompAscending);
                for(int i=0; i<(temp.size()-numSegmentsPerHnGoal)-1; i++){
                    over.add(temp.get(i));
                }
            }
            else {
                // get the full allocation of this hn
                Long allocation = 0L;
                for(Map.Entry<DataSegment, Long> e2 : hnToSegMap.get(e1.getKey()).entrySet()) {
                    allocation += e2.getValue();
                }
                MutablePair<DataSegment, DruidServerMetadata> p1 = new MutablePair<>(null, e1.getKey());
                MutablePair<MutablePair<DataSegment, DruidServerMetadata>, Long> p2 = new MutablePair<>(p1, allocation);
                under.add(p2);
            }
        }
    }

    private static final Comparator<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, MutablePair<Long, Long>>> segmentComparator =
            new Comparator<MutablePair<MutablePair<DataSegment, DruidServerMetadata>, MutablePair<Long, Long>>>()
    {
        @Override
        public int compare(MutablePair<MutablePair<DataSegment, DruidServerMetadata>, MutablePair<Long, Long>> left,
                           MutablePair<MutablePair<DataSegment, DruidServerMetadata>, MutablePair<Long, Long>> right)
        {
            return Ints.compare(left.rhs.lhs.intValue(), right.rhs.lhs.intValue());
        }
    };

    private static final Comparator<Map.Entry<DataSegment, Long>> loadComparator =
            new Comparator<Map.Entry<DataSegment, Long>>()
            {
                @Override
                public int compare(Map.Entry<DataSegment, Long> left,
                                   Map.Entry<DataSegment, Long> right)
                {
                    return Ints.compare(left.getValue().intValue(), right.getValue().intValue());
                }
            };

    private static final Comparator<Map.Entry<DruidServerMetadata, Long>> allocComparator =
            new Comparator<Map.Entry<DruidServerMetadata, Long>>()
            {
                @Override
                public int compare(Map.Entry<DruidServerMetadata, Long> left,
                                   Map.Entry<DruidServerMetadata, Long> right)
                {
                    return Ints.compare(left.getValue().intValue(), right.getValue().intValue());
                }
            };

    private static final long IMBALANCE_THRESHOLD = 30;

    public static HashMap<DataSegment,HashMap<DruidServerMetadata,Long>> balanceRoutingTableSegments(
            HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable) {
        int numSegmentReplicas = 0;
        int numHns = 0;
        int numSegmentsPerHnGoal = 0;

        // map of hn to number of segment it is storing
        HashMap<DruidServerMetadata, Integer> hnSegmentCountMap = new HashMap<DruidServerMetadata, Integer>();
        // map of hn to total cpu allocation done on that hn
        HashMap<DruidServerMetadata, Long> hnAllocMap = new HashMap<DruidServerMetadata, Long>();
        // map of hn to the segments it is storing alongwith allocation
        HashMap<DruidServerMetadata, HashMap<DataSegment, Long>> hnToSegMap = new HashMap<DruidServerMetadata, HashMap<DataSegment, Long>>();
        // balanced routing table
        HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> balancedRoutingTable =
                new HashMap<DataSegment, HashMap<DruidServerMetadata, Long>>(routingTable);

        numSegmentReplicas = buildHnSegmentMaps(hnSegmentCountMap, hnAllocMap, hnToSegMap, routingTable);
        numHns = hnAllocMap.keySet().size();
        numSegmentsPerHnGoal = Math.round((float) numSegmentReplicas / (float) numHns);

        PriorityQueue<Tuple> minheap = new PriorityQueue<Tuple>(numSegmentReplicas, new Comparator<Tuple>() {
            public int compare(Tuple o1, Tuple o2) {
                int result = Long.compare(o1.weight, o2.weight);
                return result;
            }
        });

        for (Map.Entry<DruidServerMetadata, HashMap<DataSegment, Long>> entry : hnToSegMap.entrySet()) {
            if (entry.getValue().size() > numSegmentsPerHnGoal) {
                List<Map.Entry<DataSegment, Long>> segmentEntryList = new ArrayList<>(entry.getValue().entrySet());
                Collections.sort(segmentEntryList, loadComparator);
                long numCandidates = entry.getValue().size() - numSegmentsPerHnGoal;
                for (Map.Entry<DataSegment, Long> segmentEntry : segmentEntryList) {
                    minheap.add(new Tuple(segmentEntry.getKey(), segmentEntry.getValue(), entry.getKey()));
                    numCandidates--;
                    if (numCandidates <= 0)
                        break;
                }
            }
        }

        while (minheap.peek() != null) {
            Tuple candidate = minheap.poll();
            DruidServerMetadata dstBin = findCandidateDestBin(hnAllocMap, hnToSegMap, candidate);
            if (dstBin == null)
                continue;

            double imbalance = calculateLoadImbalance(hnAllocMap, candidate, dstBin);
            log.info("Segment [%s] DstBin [%s] SrcBin [%s] Weight [%d] Imbalance [%f]",
                    candidate.segment.getIdentifier(), dstBin.getHost(), candidate.metadata.getHost(), candidate.weight, imbalance);

            if (imbalance <= IMBALANCE_THRESHOLD) {
                // Fix the allocations
                hnAllocMap.put(dstBin, hnAllocMap.get(dstBin) + candidate.weight);
                hnAllocMap.put(candidate.metadata, hnAllocMap.get(candidate.metadata) - candidate.weight);

                // Fix the routing table
                balancedRoutingTable.get(candidate.segment).remove(candidate.metadata);
                balancedRoutingTable.get(candidate.segment).put(dstBin, candidate.weight);

                // Fix the hnToSegMap
                hnToSegMap.get(candidate.metadata).remove(candidate.segment);
                hnToSegMap.get(dstBin).put(candidate.segment, candidate.weight);
            }
        }

        return balancedRoutingTable;
    }

    private static DruidServerMetadata findCandidateDestBin(HashMap<DruidServerMetadata, Long> allocMap,
                                                            HashMap<DruidServerMetadata, HashMap<DataSegment, Long>> hnToSegMap,
                                                            Tuple candidate)
    {
        log.info("AllocMap [%s]", allocMap);
        boolean first = false;
        long minLoad = 0;
        long minSegmentCount = 0;
        DruidServerMetadata dstBin = null;
        for (Map.Entry<DruidServerMetadata, Long> allocEntry : allocMap.entrySet())
        {
            if (!hnToSegMap.get(allocEntry.getKey()).containsKey(candidate.segment))
            {
                if (!first)
                {
                    minLoad = allocEntry.getValue();
                    minSegmentCount = hnToSegMap.get(allocEntry.getKey()).size();
                    dstBin = allocEntry.getKey();
                    first = true;
                }
                else if (minSegmentCount > hnToSegMap.get(allocEntry.getKey()).size())
                {
                    minLoad = allocEntry.getValue();
                    minSegmentCount = hnToSegMap.get(allocEntry.getKey()).size();
                    dstBin = allocEntry.getKey();
                }
                else if (minSegmentCount == hnToSegMap.get(allocEntry.getKey()).size())
                {
                    if (minLoad > allocEntry.getValue())
                    {
                        minLoad = allocEntry.getValue();
                        dstBin = allocEntry.getKey();
                    }
                }
            }
            else
                log.info("Server [%s]", allocEntry.getKey());
        }

        return dstBin;
    }

    private static double calculateLoadImbalance(HashMap<DruidServerMetadata, Long> allocMap, Tuple candidate, DruidServerMetadata dst)
    {
        log.info("AllocMap [%s] DstBin [%s] SrcBin [%s] Weight [%d]",
                    allocMap, dst.getHost(), candidate.metadata.getHost(), candidate.weight);
        List<Long> weights = new ArrayList<>();
        for (Map.Entry<DruidServerMetadata, Long> allocEntry : allocMap.entrySet())
        {
            if (allocEntry.getKey().equals(candidate.metadata))
                weights.add(allocEntry.getValue() - candidate.weight);
            else if (allocEntry.getKey().equals(dst))
                weights.add(allocEntry.getValue() + candidate.weight);
            else
                weights.add(allocEntry.getValue());
        }

        double imbalance = (Collections.max(weights) - Collections.min(weights)) * 100.0 / Collections.max(weights);
        return imbalance;
    }

        /* Copyright (c) 2012 Kevin L. Stern
     *
     * Permission is hereby granted, free of charge, to any person obtaining a copy
     * of this software and associated documentation files (the "Software"), to deal
     * in the Software without restriction, including without limitation the rights
     * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
     * copies of the Software, and to permit persons to whom the Software is
     * furnished to do so, subject to the following conditions:
     *
     * The above copyright notice and this permission notice shall be included in
     * all copies or substantial portions of the Software.
     *
     * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
     * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
     * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
     * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
     * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
     * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
     * SOFTWARE.
     */

    /**
     * An implementation of the Hungarian algorithm for solving the assignment
     * problem. An instance of the assignment problem consists of a number of
     * workers along with a number of jobs and a cost matrix which gives the cost of
     * assigning the i'th worker to the j'th job at position (i, j). The goal is to
     * find an assignment of workers to jobs so that no job is assigned more than
     * one worker and so that no worker is assigned to more than one job in such a
     * manner so as to minimize the total cost of completing the jobs.
     * <p>
     *
     * An assignment for a cost matrix that has more workers than jobs will
     * necessarily include unassigned workers, indicated by an assignment value of
     * -1; in no other circumstance will there be unassigned workers. Similarly, an
     * assignment for a cost matrix that has more jobs than workers will necessarily
     * include unassigned jobs; in no other circumstance will there be unassigned
     * jobs. For completeness, an assignment for a square cost matrix will give
     * exactly one unique worker to each job.
     * <p>
     *
     * This version of the Hungarian algorithm runs in time O(n^3), where n is the
     * maximum among the number of workers and the number of jobs.
     *
     * @author Kevin L. Stern
     */
    private static class HungarianAlgorithm {
        private final double[][] costMatrix;
        private final int rows, cols, dim;
        private final double[] labelByWorker, labelByJob;
        private final int[] minSlackWorkerByJob;
        private final double[] minSlackValueByJob;
        private final int[] matchJobByWorker, matchWorkerByJob;
        private final int[] parentWorkerByCommittedJob;
        private final boolean[] committedWorkers;

        /**
         * Construct an instance of the algorithm.
         *
         * @param costMatrix
         *          the cost matrix, where matrix[i][j] holds the cost of assigning
         *          worker i to job j, for all i, j. The cost matrix must not be
         *          irregular in the sense that all rows must be the same length.
         */
        public HungarianAlgorithm(double[][] costMatrix) {
            this.dim = Math.max(costMatrix.length, costMatrix[0].length);
            this.rows = costMatrix.length;
            this.cols = costMatrix[0].length;
            this.costMatrix = new double[this.dim][this.dim];
            for (int w = 0; w < this.dim; w++) {
                if (w < costMatrix.length) {
                    if (costMatrix[w].length != this.cols) {
                        throw new IllegalArgumentException("Irregular cost matrix");
                    }
                    this.costMatrix[w] = Arrays.copyOf(costMatrix[w], this.dim);
                } else {
                    this.costMatrix[w] = new double[this.dim];
                }
            }
            labelByWorker = new double[this.dim];
            labelByJob = new double[this.dim];
            minSlackWorkerByJob = new int[this.dim];
            minSlackValueByJob = new double[this.dim];
            committedWorkers = new boolean[this.dim];
            parentWorkerByCommittedJob = new int[this.dim];
            matchJobByWorker = new int[this.dim];
            Arrays.fill(matchJobByWorker, -1);
            matchWorkerByJob = new int[this.dim];
            Arrays.fill(matchWorkerByJob, -1);
        }

        /**
         * Compute an initial feasible solution by assigning zero labels to the
         * workers and by assigning to each job a label equal to the minimum cost
         * among its incident edges.
         */
        protected void computeInitialFeasibleSolution() {
            for (int j = 0; j < dim; j++) {
                labelByJob[j] = Double.POSITIVE_INFINITY;
            }
            for (int w = 0; w < dim; w++) {
                for (int j = 0; j < dim; j++) {
                    if (costMatrix[w][j] < labelByJob[j]) {
                        labelByJob[j] = costMatrix[w][j];
                    }
                }
            }
        }

        /**
         * Execute the algorithm.
         *
         * @return the minimum cost matching of workers to jobs based upon the
         *         provided cost matrix. A matching value of -1 indicates that the
         *         corresponding worker is unassigned.
         */
        public int[] execute() {
        /*
         * Heuristics to improve performance: Reduce rows and columns by their
         * smallest element, compute an initial non-zero dual feasible solution and
         * create a greedy matching from workers to jobs of the cost matrix.
         */
            reduce();
            computeInitialFeasibleSolution();
            greedyMatch();

            int w = fetchUnmatchedWorker();
            while (w < dim) {
                initializePhase(w);
                executePhase();
                w = fetchUnmatchedWorker();
            }
            int[] result = Arrays.copyOf(matchJobByWorker, rows);
            for (w = 0; w < result.length; w++) {
                if (result[w] >= cols) {
                    result[w] = -1;
                }
            }
            return result;
        }

        /**
         * Execute a single phase of the algorithm. A phase of the Hungarian algorithm
         * consists of building a set of committed workers and a set of committed jobs
         * from a root unmatched worker by following alternating unmatched/matched
         * zero-slack edges. If an unmatched job is encountered, then an augmenting
         * path has been found and the matching is grown. If the connected zero-slack
         * edges have been exhausted, the labels of committed workers are increased by
         * the minimum slack among committed workers and non-committed jobs to create
         * more zero-slack edges (the labels of committed jobs are simultaneously
         * decreased by the same amount in order to maintain a feasible labeling).
         * <p>
         *
         * The runtime of a single phase of the algorithm is O(n^2), where n is the
         * dimension of the internal square cost matrix, since each edge is visited at
         * most once and since increasing the labeling is accomplished in time O(n) by
         * maintaining the minimum slack values among non-committed jobs. When a phase
         * completes, the matching will have increased in size.
         */
        protected void executePhase() {
            while (true) {
                int minSlackWorker = -1, minSlackJob = -1;
                double minSlackValue = Double.POSITIVE_INFINITY;
                for (int j = 0; j < dim; j++) {
                    if (parentWorkerByCommittedJob[j] == -1) {
                        if (minSlackValueByJob[j] < minSlackValue) {
                            minSlackValue = minSlackValueByJob[j];
                            minSlackWorker = minSlackWorkerByJob[j];
                            minSlackJob = j;
                        }
                    }
                }
                if (minSlackValue > 0) {
                    updateLabeling(minSlackValue);
                }
                parentWorkerByCommittedJob[minSlackJob] = minSlackWorker;
                if (matchWorkerByJob[minSlackJob] == -1) {
            /*
             * An augmenting path has been found.
             */
                    int committedJob = minSlackJob;
                    int parentWorker = parentWorkerByCommittedJob[committedJob];
                    while (true) {
                        int temp = matchJobByWorker[parentWorker];
                        match(parentWorker, committedJob);
                        committedJob = temp;
                        if (committedJob == -1) {
                            break;
                        }
                        parentWorker = parentWorkerByCommittedJob[committedJob];
                    }
                    return;
                } else {
            /*
             * Update slack values since we increased the size of the committed
             * workers set.
             */
                    int worker = matchWorkerByJob[minSlackJob];
                    committedWorkers[worker] = true;
                    for (int j = 0; j < dim; j++) {
                        if (parentWorkerByCommittedJob[j] == -1) {
                            double slack = costMatrix[worker][j] - labelByWorker[worker]
                                    - labelByJob[j];
                            if (minSlackValueByJob[j] > slack) {
                                minSlackValueByJob[j] = slack;
                                minSlackWorkerByJob[j] = worker;
                            }
                        }
                    }
                }
            }
        }

        /**
         *
         * @return the first unmatched worker or {@link #dim} if none.
         */
        protected int fetchUnmatchedWorker() {
            int w;
            for (w = 0; w < dim; w++) {
                if (matchJobByWorker[w] == -1) {
                    break;
                }
            }
            return w;
        }

        /**
         * Find a valid matching by greedily selecting among zero-cost matchings. This
         * is a heuristic to jump-start the augmentation algorithm.
         */
        protected void greedyMatch() {
            for (int w = 0; w < dim; w++) {
                for (int j = 0; j < dim; j++) {
                    if (matchJobByWorker[w] == -1 && matchWorkerByJob[j] == -1
                            && costMatrix[w][j] - labelByWorker[w] - labelByJob[j] == 0) {
                        match(w, j);
                    }
                }
            }
        }

        /**
         * Initialize the next phase of the algorithm by clearing the committed
         * workers and jobs sets and by initializing the slack arrays to the values
         * corresponding to the specified root worker.
         *
         * @param w
         *          the worker at which to root the next phase.
         */
        protected void initializePhase(int w) {
            Arrays.fill(committedWorkers, false);
            Arrays.fill(parentWorkerByCommittedJob, -1);
            committedWorkers[w] = true;
            for (int j = 0; j < dim; j++) {
                minSlackValueByJob[j] = costMatrix[w][j] - labelByWorker[w]
                        - labelByJob[j];
                minSlackWorkerByJob[j] = w;
            }
        }

        /**
         * Helper method to record a matching between worker w and job j.
         */
        protected void match(int w, int j) {
            matchJobByWorker[w] = j;
            matchWorkerByJob[j] = w;
        }

        /**
         * Reduce the cost matrix by subtracting the smallest element of each row from
         * all elements of the row as well as the smallest element of each column from
         * all elements of the column. Note that an optimal assignment for a reduced
         * cost matrix is optimal for the original cost matrix.
         */
        protected void reduce() {
            for (int w = 0; w < dim; w++) {
                double min = Double.POSITIVE_INFINITY;
                for (int j = 0; j < dim; j++) {
                    if (costMatrix[w][j] < min) {
                        min = costMatrix[w][j];
                    }
                }
                for (int j = 0; j < dim; j++) {
                    costMatrix[w][j] -= min;
                }
            }
            double[] min = new double[dim];
            for (int j = 0; j < dim; j++) {
                min[j] = Double.POSITIVE_INFINITY;
            }
            for (int w = 0; w < dim; w++) {
                for (int j = 0; j < dim; j++) {
                    if (costMatrix[w][j] < min[j]) {
                        min[j] = costMatrix[w][j];
                    }
                }
            }
            for (int w = 0; w < dim; w++) {
                for (int j = 0; j < dim; j++) {
                    costMatrix[w][j] -= min[j];
                }
            }
        }

        /**
         * Update labels with the specified slack by adding the slack value for
         * committed workers and by subtracting the slack value for committed jobs. In
         * addition, update the minimum slack values appropriately.
         */
        protected void updateLabeling(double slack) {
            for (int w = 0; w < dim; w++) {
                if (committedWorkers[w]) {
                    labelByWorker[w] += slack;
                }
            }
            for (int j = 0; j < dim; j++) {
                if (parentWorkerByCommittedJob[j] != -1) {
                    labelByJob[j] -= slack;
                } else {
                    minSlackValueByJob[j] -= slack;
                }
            }
        }
    }


}
