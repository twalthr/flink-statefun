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

package org.apache.flink.statefun.sql.routers;

import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sql.SqlConstants;
import org.apache.flink.statefun.sql.messages.SqlAddStatementMessage;

public class SqlPlannerRouter implements Router<SqlAddStatementMessage> {

  @Override
  public void route(SqlAddStatementMessage message, Downstream<SqlAddStatementMessage> downstream) {
    downstream.forward(SqlConstants.SQL_PLANNER_FUNCTION, "planner", message);
  }
}
