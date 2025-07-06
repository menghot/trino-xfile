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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class XFileModule
        implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(XFileConnector.class).in(Scopes.SINGLETON);
        binder.bind(XFileMetadata.class).in(Scopes.SINGLETON);
        binder.bind(XFileClientDefault.class).in(Scopes.SINGLETON);
        binder.bind(XFileSplitManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(XFileConfig.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(XFileTable.class));
    }
}
