package io.trino.plugin.xfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class XFileCatalog {

    private String version;
    private List<XFileSchema> schemas;

    @JsonCreator
    public XFileCatalog(@JsonProperty("version") String version, @JsonProperty("schemas") List<XFileSchema> schemas) {
        this.version = version;
        this.schemas = schemas;
    }

    @JsonProperty

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }


    @JsonProperty


    public List<XFileSchema> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<XFileSchema> schemas) {
        this.schemas = schemas;
    }
}