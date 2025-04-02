package db.parser;

import db.query.Predicate;

import java.util.List;

public record QueryData(List<String> fields, List<String> tables, Predicate predicate) {

    @Override
    public String toString() {
        StringBuilder query = new StringBuilder("select ");
        for (String field : fields) {
            query.append(field).append(" ");
        }

        query = new StringBuilder(query.substring(0, query.length() - 2));
        query.append(" from ");
        for (String table : tables) {
            query.append(table).append(", ");
        }

        query = new StringBuilder(query.substring(0, query.length() - 2));
        String predString = predicate.toString();
        if (!predString.isEmpty()) {
            query.append(" where ").append(predString);
        }

        return query.toString();
    }
}
