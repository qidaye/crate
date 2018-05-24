/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.indexing;

import com.google.common.collect.ImmutableMap;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public final class UpsertResultCollectors {

    public static Collector<ShardUpsertRequestAndResponse, UpsertResults, Iterable<Row>> newRowCountCollector() {
        return new RowCountCollector();
    }

    public static Collector<ShardUpsertRequestAndResponse, UpsertResults, Iterable<Row>> newSummaryCollector(DiscoveryNode localNode) {
        return new SummaryCollector(ImmutableMap.of(
            "id", new BytesRef(localNode.getId()),
            "name", new BytesRef(localNode.getName())
        ));
    }

    private static class RowCountCollector implements Collector<ShardUpsertRequestAndResponse, UpsertResults, Iterable<Row>> {

        private final Object lock = new Object();

        @Override
        public Supplier<UpsertResults> supplier() {
            return UpsertResults::new;
        }

        @Override
        public BiConsumer<UpsertResults, ShardUpsertRequestAndResponse> accumulator() {
            return this::processShardResponse;
        }

        @Override
        public BinaryOperator<UpsertResults> combiner() {
            return (i, o) -> {
                synchronized (lock) {
                    o.addResult(i.getSuccessRowCountForNoUri());
                }
                return o;
            };
        }

        @Override
        public Function<UpsertResults, Iterable<Row>> finisher() {
            return r -> Collections.singletonList(new Row1(r.getSuccessRowCountForNoUri()));
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Sets.newHashSet(Characteristics.CONCURRENT);
        }

        void processShardResponse(UpsertResults upsertResults, ShardUpsertRequestAndResponse requestAndResponse) {
            synchronized (lock) {
                upsertResults.addResult(requestAndResponse.shardResponse.successRowCount());
            }
        }
    }

    private static class SummaryCollector implements Collector<ShardUpsertRequestAndResponse, UpsertResults, Iterable<Row>> {

        private final ImmutableMap<String, BytesRef> nodeInfo;

        private final Object lock = new Object();

        SummaryCollector(ImmutableMap<String, BytesRef> nodeInfo) {
            this.nodeInfo = nodeInfo;
        }

        @Override
        public Supplier<UpsertResults> supplier() {
            return () -> new UpsertResults(nodeInfo);
        }

        @Override
        public BiConsumer<UpsertResults, ShardUpsertRequestAndResponse> accumulator() {
            return this::processShardResponse;
        }

        @Override
        public BinaryOperator<UpsertResults> combiner() {
            return (i, o) -> {
                synchronized (lock) {
                    o.merge(i);
                }
                return o;
            };
        }

        @Override
        public Function<UpsertResults, Iterable<Row>> finisher() {
            return UpsertResults::rowIterable;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Sets.newHashSet(Characteristics.CONCURRENT);
        }

        void processShardResponse(UpsertResults upsertResults, ShardUpsertRequestAndResponse requestAndResponse) {
            synchronized (lock) {
                List<ShardResponse.Failure> failures = requestAndResponse.shardResponse.failures();
                List<ShardUpsertRequest.Item> items = requestAndResponse.shardRequest.items();
                for (int i = 0; i < failures.size(); i++) {
                    ShardUpsertRequest.Item item = items.get(i);
                    ShardResponse.Failure failure = failures.get(i);
                    BytesRef sourceUri = item.sourceUri();
                    upsertResults.addResult(sourceUri, failure == null ? null : failure.message());
                }
            }
        }
    }

    private UpsertResultCollectors() {
    }
}
