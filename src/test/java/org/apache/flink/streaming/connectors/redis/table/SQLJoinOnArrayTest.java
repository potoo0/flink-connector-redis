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

package org.apache.flink.streaming.connectors.redis.table;

import io.lettuce.core.SetArgs;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBaseV2;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;

public class SQLJoinOnArrayTest extends TestRedisConfigBaseV2 {

    @Test
    void testArrayInt() {
        List<Tuple2<Long, String>> snWithTs = List.of(
                Tuple2.of(0L, "0"),
                Tuple2.of(100L, "1"),
                Tuple2.of(200L, "2"),
                Tuple2.of(300L, "0"),
                Tuple2.of(400L, "1"));
        initSrc(null, snWithTs);
        tEnv.executeSql(StringSubstitutor.replace("""
                create table dim_redis2 (
                    data array<BIGINT>
                ) with (
                    ${__redis.common},
                    'command' = 'get',
                    'value.data.structure' = 'row',
                    'maxIdle' = '2',
                    'minIdle' = '1',
                    'lookup.cache.max-rows' = '200',
                    'lookup.cache.ttl' = '100',
                    'max.retries' = '3',
                    'sink.parallelism' = '1'
                )
                """, globalProps));
        TableResult tableResult = tEnv.executeSql("""
                select t.sn
                    , dim_device2.data as d2
                    //, ARRAY_MAX(dim_device2.data) as d2_max
                from src as t
                
                LEFT JOIN dim_redis2 for system_time as of t.proctime as dim_device2
                    ON dim_device2.data = ARRAY[CAST(t.sn AS BIGINT), CAST(CONCAT(t.sn, t.sn) AS BIGINT)]
                """);
        List<Row> rows = collect(tableResult, Duration.ofSeconds(10));
        List<String> fieldNames = List.of("sn", "d2");
        List<Tuple> expected = List.of(
                Tuple2.of("0", new Long[]{null, null}),
                Tuple2.of("1", new Long[]{1L, 1L}),
                Tuple2.of("2", new Long[]{null, 2L}),
                Tuple2.of("0", new Long[]{null, null}),
                Tuple2.of("1", new Long[]{1L, 1L})
        );

        Assertions.assertThat(rows)
                .extracting(r -> TestRedisConfigBaseV2.toTuple(fieldNames, r))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyElementsOf(expected);
    }

    @Test
    void testGet() {
        List<Tuple2<Long, String>> snWithTs = List.of(
                Tuple2.of(0L, "0"),
                Tuple2.of(1000L, "1"),
                Tuple2.of(2000L, "2"),
                Tuple2.of(3000L, "0"),
                Tuple2.of(10000L, "1"));
        initSrc("test", snWithTs);
        tEnv.executeSql(StringSubstitutor.replace("""
                create table dim_redis1 (
                    data string
                ) with (
                    ${__redis.common},
                    'command' = 'get',
                    'value.data.structure' = 'row',
                    'maxIdle' = '2',
                    'minIdle' = '1',
                    'lookup.cache.max-rows' = '100',
                    'lookup.cache.ttl' = '100',
                    'max.retries' = '3',
                    'sink.parallelism' = '1'
                )
                """, globalProps));
        // lookup.cache.max-rows set to 2, which means sn=1 will be evicted from the cache
        tEnv.executeSql(StringSubstitutor.replace("""
                create table dim_redis2 (
                    data array<string>
                ) with (
                    ${__redis.common},
                    'command' = 'get',
                    'value.data.structure' = 'row',
                    'maxIdle' = '2',
                    'minIdle' = '1',
                    'lookup.cache.max-rows' = '2',
                    'lookup.cache.ttl' = '100',
                    'max.retries' = '3',
                    'sink.parallelism' = '1'
                )
                """, globalProps));
        TableResult tableResult = tEnv.executeSql("""
                select t.sn
                    , dim_device1.data as d1
                    , dim_device2.data as d2
                    , ARRAY_MAX(dim_device2.data) as d2_max
                from src as t
                
                LEFT JOIN dim_redis1 for system_time as of t.proctime as dim_device1
                    ON dim_device1.data = CONCAT('test:', t.sn)
                
                LEFT JOIN dim_redis2 for system_time as of t.proctime as dim_device2
                    ON dim_device2.data = ARRAY[CONCAT('test:', t.sn), CONCAT('test:', t.sn, t.sn)]
                """);
        List<Row> rows = collect(tableResult, Duration.ofSeconds(20));
        List<String> fieldNames = List.of("sn", "d1", "d2", "d2_max");
        List<Tuple> expected = List.of(
                Tuple4.of("0", null, new String[]{null, null}, null),
                Tuple4.of("1", "1", new String[]{"1", "1"}, "1"),
                Tuple4.of("2", null, new String[]{null, "2"}, "2"),
                Tuple4.of("0", null, new String[]{null, null}, null),
                // sn=1 is evicted from the cache
                Tuple4.of("1", "1", new String[]{null, "1"}, "1")
        );

        Assertions.assertThat(rows)
                .extracting(r -> TestRedisConfigBaseV2.toTuple(fieldNames, r))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyElementsOf(expected);
    }

    /// redis:
    /// - keyPrefix:1 -> 1, ttl=5s
    /// - keyPrefix:11 -> 1, ttl=5s
    /// - keyPrefix:22 -> 2, ttl=10m
    void initSrc(@Nullable String keyPrefix, List<Tuple2<Long, String>> tableData) {
        SetArgs setArgs = SetArgs.Builder.ex(Duration.ofMinutes(10));
        keyPrefix = keyPrefix == null ? "" : (keyPrefix + ":");
        singleRedisCommands.set(keyPrefix + "1", "1", SetArgs.Builder.ex(Duration.ofSeconds(5)));
        singleRedisCommands.set(keyPrefix + "11", "1", SetArgs.Builder.ex(Duration.ofSeconds(5)));
        singleRedisCommands.set(keyPrefix + "22", "2", setArgs);
        ListSourceFunction sourceFunc = new ListSourceFunction(tableData, Duration.ofSeconds(0), Duration.ZERO);
        @SuppressWarnings("deprecation")
        DataStream<Row> dataStream = env.addSource(sourceFunc)
                .map((MapFunction<String, Row>) s -> {
                    Row row = Row.withNames();
                    row.setField("sn", s);
                    return row;
                })
                .returns(Types.ROW_NAMED(new String[]{"sn"}, Types.STRING));
        Schema schema = Schema.newBuilder()
                .column("sn", "STRING")
                .columnByExpression("proctime", "PROCTIME()")
                .build();
        tEnv.createTemporaryView("src", dataStream, schema);
    }
}
