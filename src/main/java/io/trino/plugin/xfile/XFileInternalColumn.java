package io.trino.plugin.xfile;

public enum XFileInternalColumn {
    FILE_PATH("__file_path__", "The URI of the data, Support dynamic filter and expression push-down"),
    ROW_NUM("__row_num__", "The row number of table"),
    ROW_TEXT("__row_text__", "The raw text of the row"),

    HTTP_URL("__http_url__", "The HTTP URL"),
    HTTP_METHOD("__http_method__", "The HTTP method"),
    HTTP_HEADERS("__http_headers__", "The HTTP header"),
    HTTP_BODY("__http_body__", "The HTTP body of the data"),
    HTTP_PARAMS("__http_params__", "The parameters to replace the placeholder parameter"),
    HTTP_PROXY_HOST("__http_proxy_host__", "The HTTP proxy host"),
    HTTP_PROXY_PORT("__http_proxy_port__", "The HTTP proxy port");

    private final String name;
    private final String description;

    XFileInternalColumn(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
