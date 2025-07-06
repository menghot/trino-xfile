package io.trino.plugin.xfile;

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.airlift.json.JsonCodec.jsonCodec;

public class XFileSplitSource implements ConnectorSplitSource {

    private final DynamicFilter dynamicFilter;
    private final List<ConnectorSplit> splits;
    private final Map<String, String> properties;
    private final XFileTable table;
    private final XFileTableHandle XFileTableHandle;
    private FixedSplitSource source;

    public XFileSplitSource(
            XFileTable table,
            XFileTableHandle XFileTableHandle,
            DynamicFilter dynamicFilter) {

        this.table = table;
        this.dynamicFilter = dynamicFilter;
        this.splits = new ArrayList<>();
        this.properties = new HashMap<>();
        this.XFileTableHandle = XFileTableHandle;
    }

    @Override
    public synchronized CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) {
        if (source == null) {
            extractDynamicFilter();

            if (properties.containsKey("__data_uri")) {

            } else if (properties.containsKey("")) {

            }


            // build splits
            if (splits.isEmpty()) {
                splits.add(new XFileSplit("http://example.org", properties, table));
                //for (URI uri : table.getSources()) {
                    //splits.add(new XFileSplit(uri.toString(), properties, table));
                //}
            }

            //

            Collections.shuffle(splits);
            source = new FixedSplitSource(splits);
        }
        return source.getNextBatch(maxSize);
    }

    private void extractDynamicFilter() {
        if (dynamicFilter == null) {
            return;
        }
        while (!dynamicFilter.isComplete()) {
            if (dynamicFilter.isAwaitable()) {
                try {
                    dynamicFilter.isBlocked().get(180, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    throw new RuntimeException("Dynamic filter execution error", e);
                } catch (TimeoutException e) {
                    throw new RuntimeException("Dynamic filter timeout", e);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Dynamic filter interrupted", e);
                }
            }
        }

        if (dynamicFilter.getCurrentPredicate().getDomains().isPresent()) {
            dynamicFilter.getCurrentPredicate().getDomains().get().forEach(this::accept);
        }
    }

    private void accept(ColumnHandle columnHandle, Domain domain) {
        XFileColumnHandle XFileColumnHandle = (XFileColumnHandle) columnHandle;
        if (domain.isSingleValue()) {
            if (domain.getSingleValue() instanceof Slice s) {
                XFileTableHandle.getFilterMap().putIfAbsent(XFileColumnHandle.getColumnName(), s.toStringUtf8());
            }
        } else {
            List<Object> values = new ArrayList<>();
            domain.getValues().getRanges().getOrderedRanges().iterator().forEachRemaining(r -> {
                if (r.isSingleValue()) {
                    if (r.getSingleValue() instanceof Slice s) {
                        values.add(s.toStringUtf8());
                    } else {
                        values.add(r.getSingleValue());
                    }
                }
            });
            XFileTableHandle.getFilterMap().putIfAbsent(XFileColumnHandle.getColumnName(), values);
            JsonCodec<XFileTableHandle> codec = jsonCodec(XFileTableHandle.class);
            System.out.println(codec.toJson(XFileTableHandle));
        }
    }


    @Override
    public void close() {
        source.close();
    }

    @Override
    public boolean isFinished() {
        return source.isFinished();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo() {
        return this.source.getTableExecuteSplitsInfo();
    }
}
