package db.parser;

import db.query.Constant;
import db.query.Expression;
import db.query.Predicate;
import db.query.Term;
import db.record.Schema;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    private final Lexer lexer;

    public Parser(String str) {
        lexer = new Lexer(str);
    }

    /**
     * Method for parsing predicates and their components
     */
    public String field() {
        return lexer.eatId();
    }

    public Constant constant() {
        if (lexer.matchStringConstant()) {
            return new Constant(lexer.eatStringConstant());
        } else {
            return new Constant(lexer.eatIntConstant());
        }
    }

    public Expression expression() {
        if (lexer.matchId()) {
            return new Expression(field());
        } else {
            return new Expression(constant());
        }
    }

    public Term term() {
        Expression lhs = expression();
        lexer.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    public Predicate predicate() {
        Predicate predicate = new Predicate(term());
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and");
            predicate.coJoinWith(predicate());
        }
        return predicate;
    }

    /**
     * Main and auxiliary methods for parsing a query
     */

    public QueryData query() {
        lexer.eatKeyword("select");
        List<String> fields = selectList();
        lexer.eatKeyword("from");
        List<String> tables = tableList();

        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            predicate = predicate();
        }

        return new QueryData(fields, tables, predicate);
    }

    private List<String> selectList() {
        List<String> fields = new ArrayList<>();
        fields.add(field());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            fields.addAll(selectList());
        }

        return fields;
    }

    private List<String> tableList() {
        List<String> tables = new ArrayList<>();
        tables.add(lexer.eatId());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            tables.addAll(tableList());
        }

        return tables;
    }

    /**
     * Methods for parsing update commands: insert, delete, modify, create
     */
    public Object update() {
        if (lexer.matchKeyword("insert")) {
            return insert();
        } else if (lexer.matchKeyword("delete")) {
            return delete();
        } else if (lexer.matchKeyword("update")) {
            return modify();
        } else {
            return create();
        }
    }

    /**
     * create methods for parsing create commands
     */

    private Object create() {
        lexer.eatKeyword("create");
        if (lexer.matchKeyword("table")) {
            return createTable();
        } else if (lexer.matchKeyword("view")) {
            return createView();
        } else {
            return createIndex();
        }
    }

    /**
     * delete method for parsing delete commands
     */
    public DeleteData delete() {
        lexer.eatKeyword("delete");
        lexer.eatKeyword("from");
        String tableName = lexer.eatId();
        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            predicate = predicate();
        }

        return new DeleteData(tableName, predicate);
    }

    /**
     * insert Method and auxiliary methods for parsing insert command
     */
    public InsertData insert() {
        lexer.eatKeyword("insert");
        lexer.eatKeyword("into");
        String tableName = lexer.eatId();
        lexer.eatDelim('(');
        List<String> fields = fieldList();
        lexer.eatDelim(')');
        lexer.eatKeyword("values");
        lexer.eatDelim('(');
        List<Constant> values = constantList();
        lexer.eatDelim(')');

        return new InsertData(tableName, fields, values);
    }

    private List<String> fieldList() {
        List<String> fields = new ArrayList<>();
        fields.add(field());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            fields.addAll(fieldList());
        }

        return fields;
    }

    private List<Constant> constantList() {
        List<Constant> values = new ArrayList<>();
        values.add(constant());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            values.addAll(constantList());
        }

        return values;
    }

    /**
     * modify method for parsing modify commands
     */
    public ModifyData modify() {
        lexer.eatKeyword("update");
        String tableName = lexer.eatId();
        lexer.eatKeyword("set");
        String fieldName = field();
        lexer.eatDelim('=');
        Expression newValue = expression();
        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            predicate = predicate();
        }

        return new ModifyData(tableName, fieldName, newValue, predicate);
    }

    /**
     * CreateTableData and auxiliary methods for parsing creating table commands
     */

    public CreateTableData createTable() {
        lexer.eatKeyword("table");
        String tableName = lexer.eatId();
        lexer.eatDelim('(');
        Schema schema = fieldDefs();
        lexer.eatDelim(')');
        return new CreateTableData(tableName, schema);
    }

    private Schema fieldDefs() {
        Schema tableSchema = fieldDef();
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            Schema schema = fieldDefs();
            tableSchema.addAll(schema);
        }

        return tableSchema;
    }

    private Schema fieldDef() {
        String fieldName = field();
        return fieldType(fieldName);
    }

    private Schema fieldType(String fieldName) {
        Schema schema = new Schema();
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int");
            schema.addIntField(fieldName);
        } else {
            lexer.eatKeyword("varchar");
            lexer.eatDelim('(');
            int strLen = lexer.eatIntConstant();
            lexer.eatDelim(')');
            schema.addStringField(fieldName, strLen);
        }

        return schema;
    }

    /**
     * createView method for parsing create view commands
     */
    public CreateViewData createView() {
        lexer.eatKeyword("view");
        String viewName = lexer.eatId();
        lexer.eatKeyword("as");
        QueryData query = query();
        return new CreateViewData(viewName, query);
    }

    /**
     * createIndex methods for parsing create index commands
     */
    public CreateIndexData createIndex() {
        lexer.eatKeyword("index");
        String indexName = lexer.eatId();
        lexer.eatKeyword("on");
        String tableName = lexer.eatId();
        lexer.eatDelim('(');
        String fieldName = field();
        lexer.eatDelim(')');

        return new CreateIndexData(indexName, tableName, fieldName);
    }
}
