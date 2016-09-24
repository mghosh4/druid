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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class SegmentCollector {
	private static final Logger log = new Logger(SegmentCollector.class);

	private final List<DataSegment> segmentList;

	private final ObjectMapper mapper;

	@Inject
	public SegmentCollector(ObjectMapper mapper){
		this.segmentList = new CopyOnWriteArrayList<>();
		this.mapper = mapper;
	}
	
	public void addSegment(DataSegment segment)
	{
		log.info("Adding Segment ID [%s]", segment.getIdentifier());
		segmentList.add(segment);
	}
	
	public String getSerializedSegmentList()
	{
		String result = null;
		try {
			result = mapper.writeValueAsString(segmentList);
			log.info("Serializing Segment List [%d]", result.length());
			segmentList.clear();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;	
	}
}
