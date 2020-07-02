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
package org.apache.flink.statefun.sql;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.statefun.sql.functions.SqlEvaluationFunction;
import org.apache.flink.statefun.sql.functions.SqlPlanProviderFunction;
import org.apache.flink.statefun.sql.functions.SqlPlannerFunction;
import org.apache.flink.statefun.sql.routers.SqlDataRouter;
import org.apache.flink.statefun.sql.routers.SqlPlannerRouter;

@AutoService(StatefulFunctionModule.class)
public class SqlModule implements StatefulFunctionModule {

  private static final int PLAN_REPLICATION_FACTOR = 256;

  private static final int PLAN_REFRESH_INTERVAL = 5_000;

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    binder.bindIngressRouter(SqlConstants.SQL_ADD_STATEMENT_INGRESS, new SqlPlannerRouter());
    binder.bindIngressRouter(SqlConstants.SQL_DATA_INGRESS, new SqlDataRouter());
    binder.bindFunctionProvider(SqlConstants.SQL_PLANNER_FUNCTION, (unused) -> new SqlPlannerFunction(PLAN_REPLICATION_FACTOR));
    binder.bindFunctionProvider(SqlConstants.SQL_PLAN_PROVIDER_FUNCTION, (unused) -> new SqlPlanProviderFunction());
    binder.bindFunctionProvider(SqlConstants.SQL_EVALUATION_FUNCTION, (unused) -> new SqlEvaluationFunction(PLAN_REPLICATION_FACTOR, PLAN_REFRESH_INTERVAL));
  }
}
