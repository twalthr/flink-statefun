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

package org.apache.flink.statefun.sql.model;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;

import java.util.Map;

public final class SqlEvaluationPlan {

	// from a to b
	private final Map<Integer, Integer> routing;

	private final Map<Integer, Object> operators;

	private final Map<Integer, TypeSerializer<RowData>> keySerializers;

	private final Map<Integer, TypeSerializer<RowData>> dataSerializers;

	private final String outputType;

	public SqlEvaluationPlan(
			Map<Integer, Integer> routing,
			Map<Integer, Object> operators,
			Map<Integer, TypeSerializer<RowData>> keySerializers,
			Map<Integer, TypeSerializer<RowData>> dataSerializers,
			String outputType) {
	  this.routing = routing;
	  this.operators = operators;
	  this.keySerializers = keySerializers;
	  this.dataSerializers = dataSerializers;
	  this.outputType = outputType;
	}

	public Map<Integer, Integer> getRouting() {
	  return routing;
	}

	public Map<Integer, Object> getOperators() {
	  return operators;
	}

	public Map<Integer, TypeSerializer<RowData>> getKeySerializers() {
		return keySerializers;
	}

	public Map<Integer, TypeSerializer<RowData>> getDataSerializers() {
		return dataSerializers;
	}

	public String getOutputType() {
		return outputType;
	}
}
