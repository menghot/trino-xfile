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

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

public class XFileConfig {
    private URI metadataUri;
    private String authentication;
    private String credentials;

    @NotNull
    public URI getMetadataUri() {
        return metadataUri;
    }


    @Config("metadata-uri")
    public XFileConfig setMetadataUri(URI metadataUri) {
        this.metadataUri = metadataUri;
        return this;
    }

    public String getAuthentication() {
        return authentication;
    }

    public String getCredentials() {
        return credentials;
    }
}
