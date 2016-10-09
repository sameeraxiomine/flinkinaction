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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;
public class ContinousEventsSource implements SourceFunction<Tuple2<String, Integer>> {
	public static int[] temperatures = {40,50,60,70,80,90,100,110,120,130};
	public static int[] pressures = {400,500,600,700,800,900,1000,1100,1200,1300};
	private volatile  boolean running = true;
	private Random rnd= new Random();
	public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
		while(running){
			sourceContext.collect(Tuple2.of("temperature",temperatures[rnd.nextInt(10)]));
			sourceContext.collect(Tuple2.of("pressure", pressures[rnd.nextInt(10)]));
			Thread.currentThread().sleep(1000);
		}
	}

	public void cancel() {
		running = false;
	}
}
