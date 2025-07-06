/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.xfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class XFileSplit
        implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(XFileSplit.class);
    private final String uri;
    private final Map<String, String> splitInfos;
    private final List<HostAddress> addresses;
    private final XFileTable XFileTable;

    @JsonCreator
    public XFileSplit(
            @JsonProperty("uri") String uri,
            @JsonProperty("properties") Map<String, String> splitInfo,
            @JsonProperty("table") XFileTable XFileTable) {
        this.uri = requireNonNull(uri, "uri is null");
        this.splitInfos = Objects.requireNonNullElseGet(splitInfo, HashMap::new);
        this.XFileTable = XFileTable;
        addresses = ImmutableList.of(HostAddress.fromUri(URI.create(uri)));
    }

    @JsonProperty
    public XFileTable getExampleTable() {
        return XFileTable;
    }

    public Map<String, String> getSplitInfos() {
        return splitInfos;
    }

    @JsonProperty
    public String getUri() {
        return uri;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE
                + estimatedSizeOf(uri);
    }
}
