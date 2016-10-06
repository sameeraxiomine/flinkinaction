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

import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RulesSource implements SourceFunction<Tuple3<String, Integer,Long>> {
	public void run(SourceContext<Tuple3<String, Integer,Long>> sourceContext) throws Exception {
		Thread.currentThread().sleep(10000);
		sourceContext.collect(Tuple3.of("temperature", 50, System.currentTimeMillis()));
		sourceContext.collect(Tuple3.of("pressure", 500, System.currentTimeMillis()));
		Thread.currentThread().sleep(5000);
		sourceContext.collect(Tuple3.of("temperature", 60, System.currentTimeMillis()));
		sourceContext.collect(Tuple3.of("pressure", 600, System.currentTimeMillis()));
	}

	public void cancel() {
	}
}
