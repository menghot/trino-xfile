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

import com.google.common.io.Resources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class XFileMetadataTest {
    private static final XFileTableHandle NUMBERS_TABLE_HANDLE = new XFileTableHandle("example", "numbers");
    private XFileMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception {
        URL metadataUrl = Resources.getResource(XFileMetadataClientFileStoreImplTest.class, "/example-data/example-metadata.json");
        assertThat(metadataUrl)
                .describedAs("metadataUrl is null")
                .isNotNull();
    }
}
