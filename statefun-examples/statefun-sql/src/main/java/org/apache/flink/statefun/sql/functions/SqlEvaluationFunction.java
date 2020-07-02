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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.statefun.flink.core.functions.ReusableContext;
import org.apache.flink.statefun.flink.core.state.FlinkState;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sql.SqlConstants;
import org.apache.flink.statefun.sql.messages.SqlPlanRequestMessage;
import org.apache.flink.statefun.sql.messages.SqlPlanResponseMessage;
import org.apache.flink.statefun.sql.messages.SqlResultMessage;
import org.apache.flink.statefun.sql.messages.SqlSourceMessage;
import org.apache.flink.statefun.sql.messages.SqlTransformationMessage;
import org.apache.flink.statefun.sql.model.SqlEvaluationPlan;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static org.apache.flink.table.utils.PrintUtils.printSingleRow;
import static org.apache.flink.table.utils.PrintUtils.rowToString;

public final class SqlEvaluationFunction implements StatefulFunction {

	private final int replicationFactor;

	private final long planRefreshInterval;

	// plan caching

	private final Map<Integer, SqlEvaluationPlan> planCache = new HashMap<>();

	private long lastUpdate = Long.MIN_VALUE;

	// state

	@Persisted
	private final PersistedTable<String, byte[]> valueState = PersistedTable.of("valueState", String.class, byte[].class);

	public SqlEvaluationFunction(int replicationFactor, long planRefreshInterval) {
		this.replicationFactor = replicationFactor;
		this.planRefreshInterval = planRefreshInterval;
	}

	@Override
	public void invoke(Context context, Object input) {
		try {
			if (lastUpdate < System.currentTimeMillis() - planRefreshInterval) {
				requestPlanUpdate(context);
			}

			if (input instanceof SqlPlanResponseMessage) {
				updateCaches((SqlPlanResponseMessage) input);
			}

			if (planCache.isEmpty()) {
				System.out.println("Waiting for plans.\n");
				return;
			}

			if (input instanceof SqlSourceMessage) {
				evaluateSourceMessage(context, (SqlSourceMessage) input);
			} else if (input instanceof SqlTransformationMessage) {
				evaluateTransformationMessage(context, (SqlTransformationMessage) input);
			}
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private void requestPlanUpdate(Context context) {
		lastUpdate = System.currentTimeMillis();
		final int targetProvider = ThreadLocalRandom.current().nextInt(0, replicationFactor);
		context.send(
				SqlConstants.SQL_PLAN_PROVIDER_FUNCTION,
				String.valueOf(targetProvider),
				SqlPlanRequestMessage.INSTANCE
		);
	}

	private void evaluateTransformationMessage(Context context, SqlTransformationMessage message) throws Exception {
		final SqlEvaluationPlan plan = planCache.get(message.getQueryId());
		evaluatePlan(
				context,
				message.getQueryId(),
				plan,
				message.getSourceOperationId(),
				message.getTargetOperationId(),
				message.getKey(),
				message.getData());
	}

	private void updateCaches(SqlPlanResponseMessage message) {
		final boolean wasEmpty = planCache.isEmpty();
		message.getPlans().forEach(planCache::putIfAbsent);
		if (wasEmpty && !planCache.isEmpty()) {
			System.out.println("Received plans.\n");
		}
	}

	private void evaluateSourceMessage(Context context, SqlSourceMessage message) throws Exception {
		for (Map.Entry<Integer, SqlEvaluationPlan> entry : planCache.entrySet()) {
			final int queryId = entry.getKey();
			final SqlEvaluationPlan plan = entry.getValue();
			// plan has no operators we can output a result directly
			if (plan.getOperators().size() == 0) {
				emitResult(context, message.getData(), LogicalTypeParser.parse(plan.getOutputType()), queryId);
			}
			// evaluate plan
			else {
				evaluatePlan(context, queryId, plan, 0, plan.getRouting().get(0), message.getKey(), message.getData());
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void evaluatePlan(
			Context context,
			int queryId,
			SqlEvaluationPlan plan,
			int sourceOperationId,
			int currentOperationId,
			byte[] keyBytes,
			byte[] dataBytes) throws Exception {
		final Object currentOperation = plan.getOperators().get(currentOperationId);

		final Supplier<RowData> key = () -> {
			try {
				final TypeSerializer<RowData> inputSerializer = plan.getKeySerializers().get(sourceOperationId);
				return InstantiationUtil.deserializeFromByteArray(inputSerializer, keyBytes);
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		};

		final Supplier<RowData> data = () -> {
			try {
				final TypeSerializer<RowData> inputSerializer = plan.getDataSerializers().get(sourceOperationId);
				return InstantiationUtil.deserializeFromByteArray(inputSerializer, dataBytes);
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		};

		final Output<StreamRecord<RowData>> output = createOutput(context, queryId, currentOperationId, keyBytes, plan);

		final Collector<RowData> collector = createCollector(context, queryId, currentOperationId, keyBytes, plan);

		// operator
		if (currentOperation instanceof StreamOperatorFactory) {
			final AbstractStreamOperatorFactory<RowData> factory = (AbstractStreamOperatorFactory<RowData>) currentOperation;
			final StreamOperatorParameters<RowData> parameters = createStreamOperatorParameters(context, output);
			factory.setProcessingTimeService(parameters.getProcessingTimeService());
			final StreamOperator<RowData> operator = factory.createStreamOperator(parameters);
			if (operator instanceof OneInputStreamOperator) {
				final OneInputStreamOperator<RowData, RowData> oneInputOperator = (OneInputStreamOperator<RowData, RowData>) operator;
				processElement(oneInputOperator, key, data, collector);
			} else {
				throw new UnsupportedOperationException();
			}
		}

		// key selector
		else if (currentOperation instanceof KeySelector) {
			final KeySelector<RowData, RowData> keySelector = (KeySelector<RowData, RowData>) currentOperation;
			final RowData keyRowData = keySelector.getKey(data.get());
			final TypeSerializer<RowData> serializer = plan.getKeySerializers().get(currentOperationId);
			final byte[] outputKey = InstantiationUtil.serializeToByteArray(serializer, keyRowData);
			final int targetOperationId = plan.getRouting().get(currentOperationId);
			// already keyed
			if (Arrays.equals(keyBytes, outputKey)) {
				evaluatePlan(context, queryId, plan, currentOperationId, targetOperationId, keyBytes, dataBytes);
			}
			// key it
			else {
				emit(context, queryId, currentOperationId, outputKey, plan, dataBytes);
			}
		}

		// other
		else {
			throw new UnsupportedOperationException();
		}
	}

	@SuppressWarnings("unchecked")
	private void processElement(
			OneInputStreamOperator<RowData, RowData> operator,
			Supplier<RowData> key,
			Supplier<RowData> data,
			Collector<RowData> collector) throws Exception {
		if (operator instanceof AbstractUdfStreamOperator) {
			final Function udf = ((AbstractUdfStreamOperator<RowData, ?>) operator).getUserFunction();
			final RuntimeContext internalContext = FunctionUtils.getFunctionRuntimeContext(udf, null);
			FunctionUtils.setFunctionRuntimeContext(udf, new SqlRuntimeContext(internalContext));
			FunctionUtils.openFunction(udf, new Configuration());
			if (udf instanceof KeyedProcessFunction) {
				final KeyedProcessFunction<RowData, RowData, RowData> processFunction = (KeyedProcessFunction<RowData, RowData, RowData>) udf;
				processFunction.processElement(data.get(), new SqlKeyedProcessFunctionContext(processFunction, key), collector);
			}
		}
		// best effort
		else {
			operator.processElement(new StreamRecord<>(data.get()));
		}
	}

	private class SqlKeyedProcessFunctionContext extends KeyedProcessFunction<RowData, RowData, RowData>.Context {

		private final Supplier<RowData> key;

		SqlKeyedProcessFunctionContext(KeyedProcessFunction<RowData, RowData, RowData> function, Supplier<RowData> key) {
			function.super();
			this.key = key;
		}

		@Override
		public Long timestamp() {
			return null;
		}

		@Override
		public TimerService timerService() {
			return new TimerService() {
				@Override
				public long currentProcessingTime() {
					return System.currentTimeMillis();
				}

				@Override
				public long currentWatermark() {
					throw new UnsupportedOperationException();
				}

				@Override
				public void registerProcessingTimeTimer(long time) {
					throw new UnsupportedOperationException();
				}

				@Override
				public void registerEventTimeTimer(long time) {
					throw new UnsupportedOperationException();
				}

				@Override
				public void deleteProcessingTimeTimer(long time) {
					throw new UnsupportedOperationException();
				}

				@Override
				public void deleteEventTimeTimer(long time) {
					throw new UnsupportedOperationException();
				}
			};
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public RowData getCurrentKey() {
			return key.get();
		}
	}

	private class SqlRuntimeContext implements RuntimeContext {

		private final RuntimeContext internalContext;

		private SqlRuntimeContext(RuntimeContext internalContext) {
			this.internalContext = internalContext;
		}

		@Override
		public String getTaskName() {
			throw new UnsupportedOperationException();
		}

		@Override
		public MetricGroup getMetricGroup() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getMaxNumberOfParallelSubtasks() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getIndexOfThisSubtask() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getAttemptNumber() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String getTaskNameWithSubtasks() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ExecutionConfig getExecutionConfig() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ClassLoader getUserCodeClassLoader() {
			return internalContext.getUserCodeClassLoader();
		}

		@Override
		public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Map<String, Accumulator<?, ?>> getAllAccumulators() {
			throw new UnsupportedOperationException();
		}

		@Override
		public IntCounter getIntCounter(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public LongCounter getLongCounter(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public DoubleCounter getDoubleCounter(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Histogram getHistogram(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean hasBroadcastVariable(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <RT> List<RT> getBroadcastVariable(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public DistributedCache getDistributedCache() {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			final String name = stateProperties.getName();
			stateProperties.initializeSerializerUnlessSet(new ExecutionConfig());
			final TypeSerializer<T> serializer = stateProperties.getSerializer();
			return new ValueState<T>() {
				@Override
				public T value() throws IOException {
					final byte[] valueBytes = valueState.get(name);
					if (valueBytes == null) {
						return null;
					}
					return InstantiationUtil.deserializeFromByteArray(serializer, valueState.get(name));
				}

				@Override
				public void update(T value) throws IOException {
					valueState.set(name, InstantiationUtil.serializeToByteArray(serializer, value));
				}

				@Override
				public void clear() {
					valueState.remove(name);
				}
			};
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			throw new UnsupportedOperationException();
		}
	}

	private static Collector<RowData> createCollector(
			Context context,
			int queryId,
			int currentOperationId,
			byte[] key,
			SqlEvaluationPlan plan) {
		return new Collector<RowData>() {
			@Override
			public void collect(RowData record) {
				emit(context, queryId, currentOperationId, key, plan, record);
			}

			@Override
			public void close() {
				// nothing to do
			}
		};
	}

	private static Output<StreamRecord<RowData>> createOutput(
			Context context,
			int queryId,
			int currentOperationId,
			byte[] key,
			SqlEvaluationPlan plan) {
		return new Output<StreamRecord<RowData>>() {
			@Override
			public void emitWatermark(Watermark mark) {
				throw new UnsupportedOperationException();
			}

			@Override
			public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void emitLatencyMarker(LatencyMarker latencyMarker) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void collect(StreamRecord<RowData> record) {
				emit(context, queryId, currentOperationId, key, plan, record.getValue());
			}

			@Override
			public void close() {
				// nothing to do
			}
		};
	}

	private static void emit(
			Context context,
			int queryId,
			int currentOperationId,
			byte[] key,
			SqlEvaluationPlan plan,
			RowData outRowData) {
		final TypeSerializer<RowData> serializer = plan.getDataSerializers().get(currentOperationId);
		final byte[] data;
		try {
			data = InstantiationUtil.serializeToByteArray(serializer, outRowData);
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
		emit(context, queryId, currentOperationId, key, plan, data);
	}

	private static void emit(
			Context context,
			int queryId,
			int currentOperationId,
			byte[] key,
			SqlEvaluationPlan plan,
			byte[] data) {
		try {
			// to next operator
			if (plan.getRouting().containsKey(currentOperationId)) {
				final int nextOperation = plan.getRouting().get(currentOperationId);
				context.send(
						SqlConstants.SQL_EVALUATION_FUNCTION,
						queryId + "_" + nextOperation + "_" + new String(key),
						new SqlTransformationMessage(
								queryId,
								currentOperationId,
								nextOperation,
								key,
								data));
			}
			// to sink
			else {
				emitResult(context, data, LogicalTypeParser.parse(plan.getOutputType()), queryId);
			}
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@SuppressWarnings("unchecked")
	private static void emitResult(Context context, byte[] data, LogicalType outputType, int queryId) throws IOException {
		final DataType dataType = TypeConversions.fromLogicalToDataType(outputType);
		final TypeSerializer<RowData> serializer = (TypeSerializer<RowData>) InternalSerializers.create(outputType);
		final RowData rowData = InstantiationUtil.deserializeFromByteArray(serializer, data);
		final DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(dataType);
		converter.open(SqlEvaluationFunction.class.getClassLoader());
		final Row row = (Row) converter.toExternal(rowData);
		context.send(SqlConstants.SQL_RESULT_EGRESS, new SqlResultMessage("query " + queryId + " " + stringifyRow(dataType, row)));
	}

	private static String stringifyRow(DataType dataType, Row row) {
		final TableSchema tableSchema = DataTypeUtils.expandCompositeTypeToSchema(dataType);
		final List<TableColumn> columns = tableSchema.getTableColumns();
		final int[] colWidths = PrintUtils.columnWidthsByType(columns, 20, "(NULL)", "op");
		final StringWriter out = new StringWriter();
		final PrintWriter writer = new PrintWriter(out);
		printSingleRow(colWidths, rowToString(row, "(NULL)", true), writer);
		return out.toString();
	}

	private StreamOperatorParameters<RowData> createStreamOperatorParameters(Context context, Output<StreamRecord<RowData>> output) throws Exception {
		final StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) ((FlinkState) ((ReusableContext) context).state).runtimeContext;
		final Field taskEnvironmentField = StreamingRuntimeContext.class.getDeclaredField("taskEnvironment");
		taskEnvironmentField.setAccessible(true);
		final Field streamConfigField = StreamingRuntimeContext.class.getDeclaredField("streamConfig");
		streamConfigField.setAccessible(true);
		final RuntimeEnvironment env = (RuntimeEnvironment) taskEnvironmentField.get(runtimeContext);
		final StreamConfig streamConfig = (StreamConfig) streamConfigField.get(runtimeContext);
		return new StreamOperatorParameters<>(new SqlStreamTask<>(env), streamConfig, output, runtimeContext::getProcessingTimeService, null);
	}

	private static class SqlStreamTask<OUT, OP extends StreamOperator<OUT>> extends StreamTask<OUT, OP> {

		public SqlStreamTask(Environment env) throws Exception {
			super(env);
		}

		@Override
		protected void init() throws Exception {

		}
	}
}
