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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.apache.flink.statefun.sql.SqlConstants;
import org.apache.flink.statefun.sql.messages.SqlAddStatementMessage;
import org.apache.flink.statefun.sql.messages.SqlPlanBroadcastMessage;
import org.apache.flink.statefun.sql.model.SqlEvaluationPlan;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.SelectSinkOperation;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlPlannerFunction implements StatefulFunction {

  private final int replicationFactor;

  @Persisted
  private final PersistedValue<Integer> nextQueryId = PersistedValue.of("nextQueryId", Integer.class);

  public SqlPlannerFunction(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  @Override
  public void invoke(Context context, Object input) {
    final SqlAddStatementMessage message = (SqlAddStatementMessage) input;
    final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().build());

    // dummy source
    env.createTemporaryView("T", env.fromTableSource(new DummyTableSource()));

    // dummy sink
    final ModifyOperation modifyOperation = new SelectSinkOperation(env.sqlQuery(message.getSqlStatement()).getQueryOperation());

    // extract transformations
    final List<Transformation<?>> transformations = ((TableEnvironmentImpl) env).getPlanner().translate(Collections.singletonList(modifyOperation));
    Transformation<?> transformation = transformations.get(0);

    // map transformations to an evaluation plan
    final SqlEvaluationPlan plan = extractSqlEvaluationPlan(transformation);

    // broadcast the plan under a query id
    final int queryId = nextQueryId.updateAndGet(c -> (c == null) ? 0 : c + 1);
    for (int i = 0; i < replicationFactor; i++) {
      context.send(SqlConstants.SQL_PLAN_PROVIDER_FUNCTION, String.valueOf(i), new SqlPlanBroadcastMessage(queryId, plan));
    }
  }

  private static SqlEvaluationPlan extractSqlEvaluationPlan(Transformation<?> transformation) {
    final Map<Integer, Integer> routing = new HashMap<>();
    final Map<Integer, Object> operators = new HashMap<>();
    try {
      extractSqlEvaluationPlan(routing, operators, transformation);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    return new SqlEvaluationPlan(routing, operators);
  }

  @SuppressWarnings("unchecked")
  private static int extractSqlEvaluationPlan(
          Map<Integer, Integer> routing,
          Map<Integer, Object> operators,
          Transformation<?> transformation) throws NoSuchFieldException, IllegalAccessException {
    if (transformation instanceof SinkTransformation) {
      // skip sink and sink conversion
      final SinkTransformation<?> sinkTransformation = (SinkTransformation<?>) transformation;
      final OneInputTransformation<?, ?> sinkConversionTransformation = ((OneInputTransformation<?, ?>) sinkTransformation.getInput());
      return extractSqlEvaluationPlan(routing, operators, sinkConversionTransformation.getInput());
    } else if (transformation instanceof OneInputTransformation) {
      final OneInputTransformation<RowData, RowData> oneInputTransformation = (OneInputTransformation<RowData, RowData>) transformation;
      // skip source and source conversion
      if (oneInputTransformation.getInput() instanceof LegacySourceTransformation) {
        return 0;
      }
      final int inputTransformation = extractSqlEvaluationPlan(routing, operators, oneInputTransformation.getInput());
      routing.put(inputTransformation, oneInputTransformation.getId());
      operators.put(oneInputTransformation.getId(), oneInputTransformation.getOperatorFactory());
      return oneInputTransformation.getId();
    } else if (transformation instanceof PartitionTransformation) {
      final PartitionTransformation<RowData> partitionTransformation = (PartitionTransformation<RowData>) transformation;
      final KeyGroupStreamPartitioner<RowData, RowData> partitioner = (KeyGroupStreamPartitioner<RowData, RowData>) partitionTransformation.getPartitioner();
      final Field field = KeyGroupStreamPartitioner.class.getDeclaredField("keySelector");
      field.setAccessible(true);
      final KeySelector<RowData, RowData> keySelector = (KeySelector<RowData, RowData>) field.get(partitioner);
      final int inputTransformation = extractSqlEvaluationPlan(routing, operators, partitionTransformation.getInput());
      routing.put(inputTransformation, partitionTransformation.getId());
      operators.put(partitionTransformation.getId(), keySelector);
      return partitionTransformation.getId();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static final class DummyTableSource implements StreamTableSource<RowData> {

    @Override
    public DataType getProducedDataType() {
      return DataTypes.ROW(
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("score", DataTypes.INT()));
    }

    @Override
    public TableSchema getTableSchema() {
      return DataTypeUtils.expandCompositeTypeToSchema(getProducedDataType());
    }

    @Override
    public DataStream getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
      return streamExecutionEnvironment.fromCollection(Collections.emptyList(), getTableSchema().toRowType());
    }
  }
}
