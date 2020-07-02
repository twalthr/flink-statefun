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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
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
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.SelectSinkOperation;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
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

    final Table table = env.sqlQuery(message.getSqlStatement());
    final DataType outputType = table.getSchema().toPhysicalRowDataType();

    // dummy sink
    final ModifyOperation modifyOperation = new SelectSinkOperation(table.getQueryOperation());

    // extract transformations
    final List<Transformation<?>> transformations = ((TableEnvironmentImpl) env).getPlanner().translate(Collections.singletonList(modifyOperation));
    Transformation<?> transformation = transformations.get(0);

    // map transformations to an evaluation plan
    final SqlEvaluationPlan plan = extractSqlEvaluationPlan(transformation, outputType);

    // broadcast the plan under a query id
    final int queryId = nextQueryId.updateAndGet(c -> (c == null) ? 0 : c + 1);
    for (int i = 0; i < replicationFactor; i++) {
      context.send(SqlConstants.SQL_PLAN_PROVIDER_FUNCTION, String.valueOf(i), new SqlPlanBroadcastMessage(queryId, plan));
    }
    System.out.println("Plan populated as query " + queryId + " for: " + message.getSqlStatement() + "\n");
  }

  private static SqlEvaluationPlan extractSqlEvaluationPlan(Transformation<?> transformation, DataType outputType) {
    final Map<Integer, Integer> routing = new HashMap<>();
    final Map<Integer, Object> operators = new HashMap<>();
    final Map<Integer, TypeSerializer<RowData>> keySerializers = new HashMap<>();
    final Map<Integer, TypeSerializer<RowData>> dataSerializers = new HashMap<>();
    try {
      extractSqlEvaluationPlan(routing, operators, keySerializers, dataSerializers, transformation);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    return new SqlEvaluationPlan(
            routing,
            operators,
            keySerializers,
            dataSerializers,
            outputType.getLogicalType().asSerializableString());
  }

  @SuppressWarnings("unchecked")
  private static int extractSqlEvaluationPlan(
          Map<Integer, Integer> routing,
          Map<Integer, Object> operators,
          Map<Integer, TypeSerializer<RowData>> keySerializers,
          Map<Integer, TypeSerializer<RowData>> dataSerializers,
          Transformation<?> transformation) throws NoSuchFieldException, IllegalAccessException {
    // sink
    if (transformation instanceof SinkTransformation) {
      // skip sink and sink conversion
      final SinkTransformation<?> sinkTransformation = (SinkTransformation<?>) transformation;
      final OneInputTransformation<?, ?> sinkConversionTransformation = ((OneInputTransformation<?, ?>) sinkTransformation.getInput());
      return extractSqlEvaluationPlan(routing, operators, keySerializers, dataSerializers, sinkConversionTransformation.getInput());
    }

    // source or one input
    else if (transformation instanceof OneInputTransformation) {
      final OneInputTransformation<RowData, RowData> oneInputTransformation = (OneInputTransformation<RowData, RowData>) transformation;
      // skip source and source conversion
      if (oneInputTransformation.getInput() instanceof LegacySourceTransformation) {
        dataSerializers.put(0, oneInputTransformation.getOutputType().createSerializer(new ExecutionConfig()));
        return 0;
      }
      final int inputTransformation = extractSqlEvaluationPlan(routing, operators, keySerializers, dataSerializers, oneInputTransformation.getInput());
      routing.put(inputTransformation, oneInputTransformation.getId());
      operators.put(oneInputTransformation.getId(), oneInputTransformation.getOperatorFactory());
      dataSerializers.put(oneInputTransformation.getId(), oneInputTransformation.getOutputType().createSerializer(new ExecutionConfig()));
      return oneInputTransformation.getId();
    }

    // partition
    else if (transformation instanceof PartitionTransformation) {
      final PartitionTransformation<RowData> partitionTransformation = (PartitionTransformation<RowData>) transformation;
      final StreamPartitioner<RowData> partitioner = partitionTransformation.getPartitioner();
      final KeySelector<RowData, RowData> keySelector;
      // hash
      if (partitioner instanceof KeyGroupStreamPartitioner) {
        final Field field = KeyGroupStreamPartitioner.class.getDeclaredField("keySelector");
        field.setAccessible(true);
        keySelector = (KeySelector<RowData, RowData>) field.get(partitioner);
      }
      // global
      else if (partitioner instanceof GlobalPartitioner) {
        keySelector = new GlobalKeySelector();
      } else {
        throw new UnsupportedOperationException();
      }

      final int inputTransformation = extractSqlEvaluationPlan(routing, operators, keySerializers, dataSerializers, partitionTransformation.getInput());
      routing.put(inputTransformation, partitionTransformation.getId());
      operators.put(partitionTransformation.getId(), keySelector);
      keySerializers.put(partitionTransformation.getId(), ((ResultTypeQueryable<RowData>) keySelector).getProducedType().createSerializer(new ExecutionConfig()));
      dataSerializers.put(partitionTransformation.getId(), partitionTransformation.getOutputType().createSerializer(new ExecutionConfig()));

      return partitionTransformation.getId();
    }

    // other
    else {
      throw new UnsupportedOperationException();
    }
  }

  private static final class GlobalKeySelector implements KeySelector<RowData, RowData>, ResultTypeQueryable<RowData> {

    @Override
    public RowData getKey(RowData value) {
      final GenericRowData data = new GenericRowData(1);
      data.setField(0, 88);
      return data;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
      return RowDataTypeInfo.of(RowType.of(new IntType()));
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
