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

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitionsAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class IndexWriterProjectorUnitTest extends CrateDummyClusterServiceUnitTest {

    private static final ColumnIdent ID_IDENT = new ColumnIdent("id");
    private static final RelationName BULK_IMPORT_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "bulk_import");
    private static final Reference RAW_SOURCE_REFERENCE = new Reference(
        new ReferenceIdent(BULK_IMPORT_IDENT, "_raw"), RowGranularity.DOC, DataTypes.STRING);
    private static final Index INDEX = new Index(BULK_IMPORT_IDENT.indexName(), UUID.randomUUID().toString());
    private static final ShardId SHARD_ID = new ShardId(INDEX, 0);

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Before
    public void setUpExecutors() throws Exception {
        scheduler = Executors.newScheduledThreadPool(1);
        executor = Executors.newFixedThreadPool(1);
    }

    @After
    public void tearDownExecutors() throws Exception {
        scheduler.shutdown();
        executor.shutdown();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testNullPKValue() throws Throwable {
        InputCollectExpression sourceInput = new InputCollectExpression(0);
        List<CollectExpression<Row, ?>> collectExpressions = Collections.<CollectExpression<Row, ?>>singletonList(sourceInput);

        TransportCreatePartitionsAction transportCreatePartitionsAction = mock(TransportCreatePartitionsAction.class);
        IndexWriterProjector indexWriter = new IndexWriterProjector(
            clusterService,
            new NodeJobsCounter(),
            scheduler,
            executor,
            TestingHelpers.getFunctions(),
            Settings.EMPTY,
            Settings.EMPTY,
            transportCreatePartitionsAction,
            (request, listener) -> {},
            IndexNameResolver.forTable(BULK_IMPORT_IDENT),
            RAW_SOURCE_REFERENCE,
            Collections.singletonList(ID_IDENT),
            Collections.<Symbol>singletonList(new InputColumn(1)),
            null,
            null,
            sourceInput,
            () -> null,
            () -> null,
            collectExpressions,
            Collections.emptyList(),
            20,
            null,
            null,
            false,
            false,
            UUID.randomUUID(),
            UpsertResultCollectors.newRowCountCollector());

        RowN rowN = new RowN(new Object[]{new BytesRef("{\"y\": \"x\"}"), null});
        BatchIterator<Row> batchIterator = InMemoryBatchIterator.of(Collections.singletonList(rowN), SENTINEL);
        batchIterator = indexWriter.apply(batchIterator);

        TestingRowConsumer testingBatchConsumer = new TestingRowConsumer();
        testingBatchConsumer.accept(batchIterator, null);

        List<Object[]> result = testingBatchConsumer.getResult();
        // zero affected rows as a NULL as a PK value will result in an error
        // the error must bubble as other rows might already have been written
        assertThat(result.get(0)[0], is(0L));
    }

    @Test
    public void testReturnSummary() throws Throwable {
        setNewClusterStateFor(createShardRouting());

        InputCollectExpression sourceInput = new InputCollectExpression(0);
        InputCollectExpression sourceUriInput = new InputCollectExpression(2);

        TransportCreatePartitionsAction transportCreatePartitionsAction = mock(TransportCreatePartitionsAction.class);
        IndexWriterProjector indexWriter = new IndexWriterProjector(
            clusterService,
            new NodeJobsCounter(),
            scheduler,
            executor,
            TestingHelpers.getFunctions(),
            Settings.EMPTY,
            Settings.EMPTY,
            transportCreatePartitionsAction,
            (request, listener) -> {
                ShardResponse shardResponse = new ShardResponse();
                for (ShardRequest.Item item : request.items()) {
                    shardResponse.add(item.location());
                }
                listener.onResponse(shardResponse);
            },
            IndexNameResolver.forTable(BULK_IMPORT_IDENT),
            RAW_SOURCE_REFERENCE,
            Collections.singletonList(ID_IDENT),
            Collections.<Symbol>singletonList(new InputColumn(1)),
            null,
            null,
            sourceInput,
            sourceUriInput,
            () -> null,
            Collections.singletonList(sourceInput),
            Collections.singletonList(sourceUriInput),
            20,
            null,
            null,
            false,
            false,
            UUID.randomUUID(),
            UpsertResultCollectors.newSummaryCollector(clusterService.localNode()));

        RowN row1 = new RowN(new Object[]{new BytesRef("{\"y\": \"foo\"}"), 1, new BytesRef("file://tmp/import1.json")});
        RowN row2 = new RowN(new Object[]{new BytesRef("{\"y\": \"bar\"}"), 2, new BytesRef("file://tmp/import2.json")});
        BatchIterator<Row> batchIterator = InMemoryBatchIterator.of(Arrays.asList(row1, row2), SENTINEL);
        batchIterator = indexWriter.apply(batchIterator);

        TestingRowConsumer testingBatchConsumer = new TestingRowConsumer();
        testingBatchConsumer.accept(batchIterator, null);

        List<Object[]> result = testingBatchConsumer.getResult();
        assertThat(printedTable(result.toArray(new Object[0][])), is(
            "{id=[6e 31], name=[6e 6f 64 65 2d 6e 61 6d 65]}| file://tmp/import2.json| 1| 0| {}\n" +
            "{id=[6e 31], name=[6e 6f 64 65 2d 6e 61 6d 65]}| file://tmp/import1.json| 1| 0| {}\n"));
    }

    private ShardRouting createShardRouting() {
        return ShardRouting.newUnassigned(SHARD_ID,
            true,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        ).initialize("n1", null, 1L)
            .moveToStarted();
    }

    private void setNewClusterStateFor(ShardRouting shardRouting) {
        ClusterState newState = ClusterState.builder(clusterService.state()).routingTable(
            RoutingTable.builder().add(
                IndexRoutingTable.builder(INDEX)
                    .addShard(shardRouting)
                    .build()
            ).build())
            .metaData(MetaData.builder(clusterService.state().metaData())
                .put(IndexMetaData.builder(INDEX.getName())
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(), true)
                .build())
            .build();
        ClusterServiceUtils.setState(clusterService, newState);
    }
}
