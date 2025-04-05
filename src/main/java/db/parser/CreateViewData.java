package db.parser;

public record CreateViewData(String viewName, QueryData queryData) {
    public String viewDef() {
        return queryData.toString();
    }
}
