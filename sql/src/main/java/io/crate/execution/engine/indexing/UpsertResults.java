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
import io.crate.data.RowN;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class UpsertResults {

    private final Map<BytesRef, Result> results = new HashMap<>(1);
    private final ImmutableMap<String, BytesRef> nodeInfo;

    UpsertResults() {
        this.nodeInfo = null;
    }

    UpsertResults(ImmutableMap<String, BytesRef> nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    void addResult(long successRowCount) {
        Result result = getResultSafe(null);
        result.successRowCount += successRowCount;
    }

    void addResult(BytesRef uri, @Nullable String failureMessage) {
        assert uri != null : "expecting URI argument not to be null";
        Result result = getResultSafe(uri);
        if (failureMessage == null) {
            result.successRowCount += 1;
        } else {
            result.errorRowCount += 1;
            result.updateErrorCount(failureMessage, 1L);
        }
    }

    void addUriFailure(BytesRef uri, String uriFailure) {
        assert uri != null : "expecting URI argument not to be null";
        Result result = getResultSafe(uri);
        result.sourceUriFailure = true;
        result.updateErrorCount(uriFailure, 1L);
    }

    private Result getResultSafe(@Nullable BytesRef uri) {
        Result result = results.get(uri);
        if (result == null) {
            result = new Result();
            results.put(uri, result);
        }
        return result;
    }

    long getSuccessRowCountForNoUri() {
        return getResultSafe(null).successRowCount;
    }

    void merge(UpsertResults other) {
        for (Map.Entry<BytesRef, Result> entry : other.results.entrySet()) {
            Result result = results.get(entry.getKey());
            if (result == null) {
                results.put(entry.getKey(), entry.getValue());
            } else {
                result.merge(entry.getValue());
            }

        }
    }

    Iterable<Row> rowIterable() {
        Stream<Row> s = results.entrySet().stream()
            .map(e -> {
                Result r = e.getValue();
                if (r.sourceUriFailure) {
                    return new RowN(new Object[]{nodeInfo, e.getKey(), null, null, r.errors});
                } else {
                    return new RowN(new Object[]{nodeInfo, e.getKey(), r.successRowCount, r.errorRowCount, r.errors});
                }
            });
        return s::iterator;
    }

    static class Result {

        private static final String ERROR_COUNT_KEY = "count";

        long successRowCount = 0;
        long errorRowCount = 0;
        Map<String, Map<String, Object>> errors = new HashMap<>();
        boolean sourceUriFailure = false;

        void merge(Result upsertResult) {
            successRowCount += upsertResult.successRowCount;
            errorRowCount += upsertResult.errorRowCount;
            if (sourceUriFailure == false) {
                sourceUriFailure = upsertResult.sourceUriFailure;
            }
            for (Map.Entry<String, Map<String, Object>> entry : upsertResult.errors.entrySet()) {
                updateErrorCount(entry.getKey(), (Long) entry.getValue().get(ERROR_COUNT_KEY));
            }
        }

        private void updateErrorCount(String msg, Long increaseBy) {
            Map<String, Object> errorEntry = errors.get(msg);
            Long cnt = 0L;
            if (errorEntry == null) {
                errorEntry = new HashMap<>(1);
                errors.put(msg, errorEntry);
            } else {
                cnt = (Long) errorEntry.get(ERROR_COUNT_KEY);
            }
            errorEntry.put(ERROR_COUNT_KEY, cnt + increaseBy);
        }
    }
}
