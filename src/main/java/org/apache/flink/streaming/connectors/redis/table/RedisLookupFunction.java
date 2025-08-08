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

import io.lettuce.core.RedisFuture;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisJoinCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisSelectCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisJoinConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory.CACHE_SEPARATOR;

/**
 * redis lookup function. @Author: jeff.zou @Date: 2022/3/7.14:33
 */
public class RedisLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private RedisCommand redisCommand;
    private FlinkConfigBase flinkConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;
    private final List<DataType> dataTypes;
    private final boolean loadAll;
    private final RedisValueDataStructure redisValueDataStructure;
    /// directly store redis command output.
    private Cache<String, Object> cache;
    private RedisJoinCommandExecutor joinCommandExecutor;

    public RedisLookupFunction(
            FlinkConfigBase flinkConfigBase,
            RedisMapper<?> redisMapper,
            RedisJoinConfig redisJoinConfig,
            ResolvedSchema resolvedSchema,
            ReadableConfig readableConfig) {
        Preconditions.checkNotNull(
                flinkConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.cacheTtl = redisJoinConfig.getCacheTtl();
        this.cacheMaxSize = redisJoinConfig.getCacheMaxSize();
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.loadAll = redisJoinConfig.getLoadAll();
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);

        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();

        this.dataTypes = resolvedSchema.getColumnDataTypes();
        this.validate();
    }

    private void validate() {
        boolean hasArrayType = false;
        for (DataType dataType : this.dataTypes) {
            if (dataType.getChildren().isEmpty()) continue;
            hasArrayType = true;
            if (!RedisValueDataStructure.row.equals(this.redisValueDataStructure)
                    || dataType.getChildren().size() != 1
                    || !LogicalTypeRoot.VARCHAR.equals(dataType.getChildren().get(0).getLogicalType().getTypeRoot())) {
                throw new FlinkRuntimeException("Constructured Data Type only support Array<String> and `value.data.structure` must be row! eg: `create table dim_redis ( data Array<String> ) with ( ... )`");
            }
        }
        if (hasArrayType && this.dataTypes.size() > 1) {
            throw new FlinkRuntimeException("Array only working with single field! eg: `create table dim_redis ( data Array<String> ) with ( ... )`");
        }
    }

    public void eval(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys)
            throws Exception {

        joinCommandExecutor.eval(resultFuture, keys);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        Preconditions.checkArgument(
                redisCommand.getJoinCommand() != RedisJoinCommand.NONE,
                String.format("the command %s do not support join.", redisCommand.name()));

        if (this.loadAll) {
            Preconditions.checkArgument(
                    cacheMaxSize != -1 && cacheTtl != -1,
                    "cache must be opened by cacheMaxSize and cacheTtl when you want to load all elements to cache.");
            Preconditions.checkArgument(
                    redisCommand.getJoinCommand() == RedisJoinCommand.HGET,
                    "just data structure is Map of redis support load all.");
        }

        if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
            Preconditions.checkArgument(
                    dataTypes.get(1).getLogicalType() instanceof DoubleType,
                    "the second column's type of join table must be double. the type of score is double when the data structure in redis is SortedSet.");
        }

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container.");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }

        this.cache =
                cacheMaxSize == -1 || cacheTtl == -1
                        ? null
                        : CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheTtl, TimeUnit.SECONDS)
                        .maximumSize(cacheMaxSize)
                        .build();

        this.joinCommandExecutor = switch (redisCommand.getJoinCommand()) {
            case GET -> new RedisGetJoinCommandExecutor();
            case HGET -> new RedisHGetJoinCommandExecutor();
            case ZSCORE -> new RedisZScoreJoinCommandExecutor();
            default -> throw new UnsupportedOperationException(("Unsupported join command: " + redisCommand.name()));
        };
    }

    @Override
    public void close() throws Exception {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }

        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <T> T getCache(String key) {
        return cache == null
                ? null
                : (T) cache.getIfPresent(key);
    }

    public void retry(Runnable runnable) throws InterruptedException {
        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                runnable.run();
                break;
            } catch (Exception e) {
                LOG.error("query redis error, retry times:{}", i, e);
                if (i >= maxRetryTimes) {
                    throw new RuntimeException("query redis error ", e);
                }
                Thread.sleep(500L * i);
            }
        }
    }

    public interface RedisJoinCommandExecutor {
        void eval(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) throws InterruptedException;
    }

    public class RedisGetJoinCommandExecutor implements RedisJoinCommandExecutor {
        @Override
        public void eval(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) throws InterruptedException {
            // for array data type
            if (keys[0] instanceof ArrayData array) {
                Map<String, Integer> missingKeys = new HashMap<>();
                String[] results = new String[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    String key = String.valueOf(array.getString(i));
                    String val = getCache(key);
                    results[i] = val;
                    if (val == null) {
                        missingKeys.put(key, i);
                    }
                }
                if (missingKeys.isEmpty()) {
                    triggerFuture(resultFuture, results);
                } else {
                    retry(() -> query(resultFuture, missingKeys, results));
                }
                return;
            }

            // for scalar data type
            String result = getCache(String.valueOf(keys[0]));
            if (result != null) {
                triggerFuture(resultFuture, result, keys);
            } else {
                retry(() -> query(resultFuture, keys));
            }
        }

        private void query(CompletableFuture<Collection<GenericRowData>> resultFuture, Map<String, Integer> missingKeys, String[] results) {
            List<String> keys = new ArrayList<>(missingKeys.keySet());
            List<CompletableFuture<String>> futures = keys.stream()
                    .map(redisCommandsContainer::get)
                    .map(RedisFuture::toCompletableFuture)
                    .toList();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenAccept(v -> {
                        for (int i = 0; i < futures.size(); i++) {
                            String value = futures.get(i).join();
                            String key = keys.get(i);
                            Integer resultIdx = missingKeys.get(key);
                            results[resultIdx] = value;
                            if (cache != null && value != null) {
                                cache.put(key, value);
                            }
                        }

                        triggerFuture(resultFuture, results);
                    });
        }

        private void query(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) {
            redisCommandsContainer
                    .get(String.valueOf(keys[0]))
                    .thenAccept(
                            result -> {
                                triggerFuture(resultFuture, result, keys);
                                if (cache != null && result != null) {
                                    cache.put(String.valueOf(keys[0]), result);
                                }
                            });
        }

        private void triggerFuture(CompletableFuture<Collection<GenericRowData>> future, String result, Object... keys) {
            GenericRowData rowData = RedisResultWrapper.createRowDataForString(keys, result, redisValueDataStructure, dataTypes);
            future.complete(List.of(rowData));
        }

        private void triggerFuture(CompletableFuture<Collection<GenericRowData>> future, String[] result) {
            GenericRowData rowData = RedisResultWrapper.createRowDataForArray(result, redisValueDataStructure, dataTypes);
            future.complete(Collections.singleton(rowData));
        }

    }

    public class RedisHGetJoinCommandExecutor implements RedisJoinCommandExecutor {
        @Override
        public void eval(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) throws InterruptedException {
            // load all kv
            if (loadAll) {
                Map<String, String> map = getCache(String.valueOf(keys[0]));
                if (map != null) {
                    triggerFuture(resultFuture, map.get(String.valueOf(keys[1])), keys);
                } else {
                    retry(() -> queryAll(resultFuture, keys));
                }
                return;
            }

            // load single kv
            String key = keys[0] + CACHE_SEPARATOR + keys[1];
            String result = getCache(key);
            if (result != null) {
                triggerFuture(resultFuture, result, keys);
            } else {
                retry(() -> query(resultFuture, keys));
            }
        }

        private void queryAll(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) {
            redisCommandsContainer
                    .hgetAll(String.valueOf(keys[0]))
                    .thenAccept(
                            map -> {
                                if (map == null) {
                                    triggerFuture(resultFuture, null, keys);
                                    return;
                                }
                                triggerFuture(resultFuture, map.get(String.valueOf(keys[1])), keys);
                                if (cache != null) {
                                    cache.put(String.valueOf(keys[0]), map);
                                }
                            });
        }

        private void query(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) {
            redisCommandsContainer
                    .hget(String.valueOf(keys[0]), String.valueOf(keys[1]))
                    .thenAccept(
                            result -> {
                                triggerFuture(resultFuture, result, keys);
                                if (cache != null && result != null) {
                                    String key = keys[0] + CACHE_SEPARATOR + keys[1];
                                    cache.put(key, result);
                                }
                            });

        }

        private void triggerFuture(CompletableFuture<Collection<GenericRowData>> future, String result, Object... keys) {
            GenericRowData rowData = RedisResultWrapper.createRowDataForHash(keys, result, redisValueDataStructure, dataTypes);
            future.complete(List.of(rowData));
        }
    }


    public class RedisZScoreJoinCommandExecutor implements RedisJoinCommandExecutor {
        @Override
        public void eval(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) throws InterruptedException {
            String key = keys[0] + CACHE_SEPARATOR + keys[1];
            Double result = getCache(key);
            if (result != null) {
                triggerFuture(resultFuture, result, keys);
            } else {
                retry(() -> query(resultFuture, keys));
            }
        }

        private void query(CompletableFuture<Collection<GenericRowData>> resultFuture, Object... keys) {
            redisCommandsContainer
                    .zscore(String.valueOf(keys[0]), String.valueOf(keys[1]))
                    .thenAccept(
                            result -> {
                                triggerFuture(resultFuture, result, keys);
                                if (cache != null && result != null) {
                                    String key = keys[0] + CACHE_SEPARATOR + keys[1];
                                    cache.put(key, result);
                                }
                            });
        }

        private void triggerFuture(CompletableFuture<Collection<GenericRowData>> future, Double result, Object... keys) {
            GenericRowData rowData = RedisResultWrapper.createRowDataForSortedSet(keys, result, dataTypes);
            future.complete(List.of(rowData));
        }

    }
}
