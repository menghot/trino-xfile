package io.trino.plugin.example;

public enum ExampleInternalColumn {

    DATA_URI("$data_uri", "The URI of the data, Support dynamic filter and expression push-down"),
    ROW_NUM("$row_num", "The row number of table"),
    ROW_TEXT("$row_text", "The raw text of the row"),
    HTTP_URL("$http_url", "The HTTP URL"),
    HTTP_HEADER("$http_header", "The HTTP header"),
    HTTP_BODY("$http_body", "The HTTP body of the data"),

    PARAMS("$params", "The parameters to replace the placeholder parameter");

    private final String name;
    private final String description;

    ExampleInternalColumn(String name, String description) {
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
