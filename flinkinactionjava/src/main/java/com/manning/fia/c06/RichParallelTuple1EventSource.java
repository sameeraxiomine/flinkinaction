/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.manning.fia.c06;


import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class RichParallelTuple1EventSource extends RichParallelSourceFunction<Tuple1<Integer>> {
	
	private int noOfEvents = 0;
	private int index = -1;
	public RichParallelTuple1EventSource(int noOfEvents) {
		this.noOfEvents = noOfEvents;
	}

	@Override
	public void open(Configuration configuration) {
		this.index = getRuntimeContext().getIndexOfThisSubtask();		
	}

	public void run(SourceContext<Tuple1<Integer>> sourceContext) throws Exception {
		for(int i=0;i<this.noOfEvents;i++){
			
			sourceContext.collect(Tuple1.of(this.index));
		}
	}

	public void cancel() {
		/*
		 * Typically used to stop the source function 
		 * by setting a boolean variable to false
		 */
	}

}
