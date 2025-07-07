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

package org.apache.flink.streaming.connectors.redis.table.base;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author: Jeff Zou @Date: 2022/10/14 10:07
 */
public class TestRedisConfigBaseV2 {
    private static final Logger LOG = LoggerFactory.getLogger(TestRedisConfigBaseV2.class);
    protected static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(4);

    protected static Properties globalProps;
    protected static StatefulRedisConnection<String, String> singleConnect;
    protected static RedisCommands<String, String> singleRedisCommands;
    protected static StreamExecutionEnvironment env;
    protected static StreamTableEnvironment tEnv;
    private static RedisClient redisClient;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @BeforeAll
    public static void init() {
        globalProps = loadProperties("local.properties");
        Configuration configuration = Configuration.fromMap(globalProps.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())));
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        tEnv = StreamTableEnvironment.create(env);
        RedisURI redisURI =
                RedisURI.builder()
                        .withHost(globalProps.getProperty("redis.host", "127.0.0.1"))
                        .withPort(Integer.parseInt(globalProps.getProperty("redis.port", "6379")))
                        .withPassword(globalProps.getProperty("redis.password", "").toCharArray())
                        .withDatabase(Integer.parseInt(globalProps.getProperty("redis.database", "0")))
                        .build();
        redisClient = RedisClient.create(redisURI);
        singleConnect = redisClient.connect();
        singleRedisCommands = singleConnect.sync();
        LOG.info("connect to the redis: {}", redisURI);
    }

    @AfterAll
    public static void stopSingle() {
        singleConnect.close();
        redisClient.shutdown();
    }

    public static Tuple toTuple(List<String> fieldNames, Row row) {
        Tuple tuple = Tuple.newInstance(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            tuple.setField(row.getField(fieldNames.get(i)), i);
        }
        return tuple;
    }

    protected List<Row> collect(TableResult result, Duration duration) {
        EXECUTOR.schedule(() -> result.getJobClient().ifPresent(JobClient::cancel), duration.toSeconds(), TimeUnit.SECONDS);
        List<Row> rows = new ArrayList<>();
        try {
            try (CloseableIterator<Row> it = result.collect()) {
                it.forEachRemaining(rows::add);
            }
        } catch (Exception e) {
            if (Optional.of(e).map(Throwable::getCause).map(Throwable::getCause).map(Throwable::getCause)
                    .filter(ex -> ex instanceof JobCancellationException).isEmpty()) {
                throw new RuntimeException(e);
            }
        }
        return rows;
    }

    public static Properties loadProperties(String filePath) {
        Properties properties = new Properties();

        // First try to load from classpath
        try (InputStream input = TestRedisConfigBaseV2.class.getClassLoader().getResourceAsStream(filePath)) {
            if (input != null) {
                properties.load(input);
                LOG.info("Loaded properties from classpath: {}", filePath);
                return properties;
            }
        } catch (IOException e) {
            LOG.warn("Failed to load properties from classpath: {}", filePath, e);
        }

        // Then try to load from file system
        Path path = Paths.get(filePath);
        if (Files.exists(path)) {
            try (InputStream input = Files.newInputStream(path)) {
                properties.load(input);
                LOG.info("Loaded properties from file system: {}", filePath);
                return properties;
            } catch (IOException e) {
                LOG.error("Failed to load properties from file system: {}", filePath, e);
            }
        } else {
            LOG.warn("Properties file not found: {}", filePath);
        }

        return properties;
    }
}
