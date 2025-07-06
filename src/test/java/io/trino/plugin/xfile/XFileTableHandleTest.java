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

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class XFileTableHandleTest {
    private final XFileTableHandle tableHandle = new XFileTableHandle("schemaName", "tableName");

    @Test
    public void testJsonRoundTrip() {
        JsonCodec<XFileTableHandle> codec = jsonCodec(XFileTableHandle.class);
        String json = codec.toJson(tableHandle);
        XFileTableHandle copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(tableHandle);
    }


    @Test
    public void testJsonRoundTrip2() {
        JsonCodec<XFileTableHandle> codec = jsonCodec(XFileTableHandle.class);

        XFileTableHandle table = new XFileTableHandle("schemaName", "tableName");
        table.getFilterMap().putAll(Map.of("name", "simon", "lists", List.of("p1", "p2")));

        String json = codec.toJson(table);
        System.out.println(json);

        XFileTableHandle copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(table);
    }

    @Test
    public void testEquivalence() {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new XFileTableHandle("schema", "table"), new XFileTableHandle("schema", "table"))
                .addEquivalentGroup(new XFileTableHandle("schemaX", "table"), new XFileTableHandle("schemaX", "table"))
                .addEquivalentGroup(new XFileTableHandle("schema", "tableX"), new XFileTableHandle("schema", "tableX"))
                .check();
    }
}
