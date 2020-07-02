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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.sql.messages.SqlAddStatementMessage;
import org.apache.flink.statefun.sql.messages.SqlSourceMessage;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

public class RunnerTest {

  @Test
  public void run() throws Exception {
    final List<String> statements = Arrays.asList(
      // "SELECT * FROM T"
      //"SELECT 'The name is ' || name, score * 100 FROM T"
      //"SELECT name, COUNT(*) FROM T GROUP BY name"
      //"SELECT AVG(score) FROM T"
    );

    final List<Row> rows = Arrays.asList(
            Row.of("Timo", 13),
            Row.of("Alice", 2),
            Row.of("Bob", 0),
            Row.of("Timo", 20),
            Row.of("Bob", 0),
            Row.of("Bob", 2),
            Row.of("Timo", 30),
            Row.of("Alice", 10),
            Row.of("Alice", 1),
            Row.of("Bob", 0),
            Row.of("Timo", 20),
            Row.of("Bob", 0),
            Row.of("Bob", 2),
            Row.of("Timo", 30),
            Row.of("Alice", 10),
            Row.of("Alice", 1)
    );

    Harness harness =
        new Harness()
            .withKryoMessageSerializer()
            .withFlinkSourceFunction(SqlConstants.SQL_ADD_STATEMENT_INGRESS, new SqlStatementSourceFunction(statements))
            .withFlinkSourceFunction(SqlConstants.SQL_DATA_INGRESS, new SqlDataSourceFunction(rows))
//            .withFlinkSourceFunction(SqlConstants.SQL_ADD_STATEMENT_INGRESS, new SqlStatementSocketSourceFunction())
//            .withFlinkSourceFunction(SqlConstants.SQL_DATA_INGRESS, new SqlDataSocketSourceFunction())
            .withPrintingEgress(SqlConstants.SQL_RESULT_EGRESS);

    harness.start();
  }

  public static final class SqlStatementSocketSourceFunction implements SourceFunction<SqlAddStatementMessage> {

    private Socket currentSocket;

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<SqlAddStatementMessage> ctx) throws Exception {
      while (isRunning) {
			// open and consume from socket
			try (final Socket socket = new Socket()) {
				currentSocket = socket;
				socket.connect(new InetSocketAddress("localhost", 9999), 0);
				try (InputStream stream = socket.getInputStream()) {
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					int b;
					while ((b = stream.read()) >= 0) {
						// buffer until delimiter
						if (b != 10) {
							buffer.write(b);
						}
						// decode and emit record
						else {
							ctx.collect(new SqlAddStatementMessage(new String(buffer.toByteArray())));
							buffer.reset();
						}
					}
				}
			} catch (Throwable t) {
				t.printStackTrace(); // print and continue
			}
			Thread.sleep(1000);
		}
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

  public static final class SqlDataSocketSourceFunction implements SourceFunction<SqlSourceMessage> {

    private Socket currentSocket;

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<SqlSourceMessage> ctx) throws Exception {
      while (isRunning) {
			// open and consume from socket
			try (final Socket socket = new Socket()) {
				currentSocket = socket;
				socket.connect(new InetSocketAddress("localhost", 9998), 0);
				try (InputStream stream = socket.getInputStream()) {
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					int b;
					while ((b = stream.read()) >= 0) {
						// buffer until delimiter
						if (b != 10) {
							buffer.write(b);
						}
						// decode and emit record
						else {
							final DataType dataType = DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING()),
									DataTypes.FIELD("score", DataTypes.INT()));

						  // data
                          final String[] string = new String(buffer.toByteArray()).split(",");
						  final Row row = Row.of(string[0], Integer.valueOf(string[1].trim()));
						  final DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(dataType);
						  converter.open(this.getClass().getClassLoader());
						  final RowData data = (RowData) converter.toInternal(row);
						  final TypeSerializer<RowData> dataSerializer = (TypeSerializer<RowData>) InternalSerializers.create(dataType.getLogicalType());
						  final byte[] dataBytes = InstantiationUtil.serializeToByteArray(dataSerializer, data);

						  // key
						  final RowDataKeySelector keySelector = KeySelectorUtil.getRowDataSelector(
								  new int[]{0},
								  RowDataTypeInfo.of((RowType) dataType.getLogicalType()));
						  final RowData key = keySelector.getKey(data);
						  final TypeSerializer<RowData> keySerializer = (TypeSerializer<RowData>) InternalSerializers.create(keySelector.getProducedType().toRowType());
						  final byte[] keyBytes = InstantiationUtil.serializeToByteArray(keySerializer, key);

						    ctx.collect(new SqlSourceMessage(keyBytes, dataBytes));
							buffer.reset();
						}
					}
				}
			} catch (Throwable t) {
				t.printStackTrace(); // print and continue
			}
			Thread.sleep(1000);
		}
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

  public static final class SqlStatementSourceFunction implements SourceFunction<SqlAddStatementMessage> {

    private final List<String> statements;

    private int currentStatement;

    private volatile boolean isRunning = true;

    public SqlStatementSourceFunction(List<String> statements) {
      this.statements = statements;
    }

    @Override
    public void run(SourceContext<SqlAddStatementMessage> ctx) throws Exception {
      while (isRunning) {
        if (currentStatement < statements.size()) {
          ctx.collect(new SqlAddStatementMessage(statements.get(currentStatement++)));
        }
        Thread.sleep(10_000);
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

  public static final class SqlDataSourceFunction implements SourceFunction<SqlSourceMessage> {

    private final List<Row> rows;

    private int currentRow;

    private volatile boolean isRunning = true;

    public SqlDataSourceFunction(List<Row> rows) {
      this.rows = rows;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run(SourceContext<SqlSourceMessage> ctx) throws Exception {
      while (isRunning) {
        Thread.sleep(3_000);

        if (currentRow < rows.size()) {
          final DataType dataType = DataTypes.ROW(
                  DataTypes.FIELD("name", DataTypes.STRING()),
                  DataTypes.FIELD("score", DataTypes.INT()));

          // data
          final Row row = rows.get(currentRow++);
          final DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(dataType);
          converter.open(this.getClass().getClassLoader());
          final RowData data = (RowData) converter.toInternal(row);
          final TypeSerializer<RowData> dataSerializer = (TypeSerializer<RowData>) InternalSerializers.create(dataType.getLogicalType());
          final byte[] dataBytes = InstantiationUtil.serializeToByteArray(dataSerializer, data);

          // key
          final RowDataKeySelector keySelector = KeySelectorUtil.getRowDataSelector(
                  new int[]{0},
                  RowDataTypeInfo.of((RowType) dataType.getLogicalType()));
          final RowData key = keySelector.getKey(data);
          final TypeSerializer<RowData> keySerializer = (TypeSerializer<RowData>) InternalSerializers.create(keySelector.getProducedType().toRowType());
          final byte[] keyBytes = InstantiationUtil.serializeToByteArray(keySerializer, key);

          ctx.collect(new SqlSourceMessage(keyBytes, dataBytes));
        }
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
