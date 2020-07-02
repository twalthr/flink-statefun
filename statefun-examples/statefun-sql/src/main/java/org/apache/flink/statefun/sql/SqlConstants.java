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

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sql.messages.SqlAddStatementMessage;
import org.apache.flink.statefun.sql.messages.SqlSourceMessage;
import org.apache.flink.statefun.sql.messages.SqlResultMessage;

public final class SqlConstants {

  public static final IngressIdentifier<SqlAddStatementMessage> SQL_ADD_STATEMENT_INGRESS =
      new IngressIdentifier<>(
              SqlAddStatementMessage.class,
              "org.apache.flink.statefun.sql",
              "sql-add-statement-ingress");

  public static final IngressIdentifier<SqlSourceMessage> SQL_DATA_INGRESS =
      new IngressIdentifier<>(
              SqlSourceMessage.class,
              "org.apache.flink.statefun.sql",
              "sql-data-ingress");

  public static final EgressIdentifier<SqlResultMessage> SQL_RESULT_EGRESS =
      new EgressIdentifier<>(
              "org.apache.flink.statefun.sql",
              "sql-result-egress",
              SqlResultMessage.class);

  public static final FunctionType SQL_PLANNER_FUNCTION =
      new FunctionType(
              "org.apache.flink.statefun.sql",
              "sql-planner-function");

  public static final FunctionType SQL_PLAN_PROVIDER_FUNCTION =
      new FunctionType(
              "org.apache.flink.statefun.sql",
              "sql-plan-provider-function");

  public static final FunctionType SQL_EVALUATION_FUNCTION =
      new FunctionType(
              "org.apache.flink.statefun.sql",
              "sql-evaluation-function");
}
