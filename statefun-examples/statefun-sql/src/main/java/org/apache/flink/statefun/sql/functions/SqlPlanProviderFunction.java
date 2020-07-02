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

package org.apache.flink.statefun.sql.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sql.messages.SqlPlanBroadcastMessage;
import org.apache.flink.statefun.sql.messages.SqlPlanRequestMessage;
import org.apache.flink.statefun.sql.messages.SqlPlanResponseMessage;
import org.apache.flink.statefun.sql.model.SqlEvaluationPlan;

import java.util.HashMap;
import java.util.Map;

public final class SqlPlanProviderFunction implements StatefulFunction {

	@Persisted
	private final PersistedTable<Integer, SqlEvaluationPlan> planTable = PersistedTable.of("planTable", Integer.class, SqlEvaluationPlan.class);

	@Override
	public void invoke(Context context, Object input) {
		if (input instanceof SqlPlanBroadcastMessage) {
			final SqlPlanBroadcastMessage message = (SqlPlanBroadcastMessage) input;
			planTable.set(message.getQueryId(), message.getPlan());
		} else if (input instanceof SqlPlanRequestMessage) {
			final Map<Integer, SqlEvaluationPlan> plans = new HashMap<>();
			for (Map.Entry<Integer, SqlEvaluationPlan> entry : planTable.entries()) {
				plans.put(entry.getKey(), entry.getValue());
			}
			context.reply(new SqlPlanResponseMessage(plans));
		}
	}
}
