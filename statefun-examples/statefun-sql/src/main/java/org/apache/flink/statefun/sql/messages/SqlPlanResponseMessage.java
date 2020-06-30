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

package org.apache.flink.statefun.sql.messages;

import org.apache.flink.statefun.sql.model.SqlEvaluationPlan;

import java.util.Map;

public final class SqlPlanResponseMessage {

	private final Map<Integer, SqlEvaluationPlan> newPlans;

	public SqlPlanResponseMessage(Map<Integer, SqlEvaluationPlan> newPlans) {
		this.newPlans = newPlans;
	}

	public Map<Integer, SqlEvaluationPlan> getNewPlans() {
		return newPlans;
	}
}
