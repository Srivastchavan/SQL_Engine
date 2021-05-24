package dbProject;
import java.util.ArrayList;
import java.util.Arrays;

public class Column {
    public String tableName;
    public String name;
    public DataType type;
    public boolean unique = false;
    public boolean notNull = false;
    public boolean primaryKey = false;
    public Short ordinalPosition;

    public Column(String tableName, String columnString, short ordinalPosition) {
        this.tableName = tableName;
        ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(columnString.trim().split(" ")));
        name = columnTokens.get(0).strip();
        switch (columnTokens.get(1).toUpperCase()) {
            case "TINYINT":
                type = DataType.TINYINT;
                break;
            case "SMALLINT":
                type = DataType.SMALLINT;
                break;
            case "INT":
                type = DataType.INT;
                break;
            case "BIGINT":
                type = DataType.BIGINT;
                break;
            case "FLOAT":
                type = DataType.FLOAT;
                break;
            case "DOUBLE":
                type = DataType.DOUBLE;
                break;
            case "YEAR":
                type = DataType.YEAR;
                break;
            case "TIME":
                type = DataType.TIME;
                break;
            case "DATETIME":
                type = DataType.DATETIME;
                break;
            case "DATE":
                type = DataType.DATE;
                break;
            case "TEXT":
            	type = DataType.TEXT;
            	break;
            default:
                type = DataType.NULL;
                break;
        }
        String options = "";
        for (String columnOptionToken : columnTokens.subList(2, columnTokens.size())) {
            options += columnOptionToken + " ";
        }
        options = options.toUpperCase();
        unique = options.contains("UNIQUE");
        notNull = options.contains("NOT NULL");
        primaryKey = options.contains("PRIMARY KEY");
        this.ordinalPosition = ordinalPosition;
    }

    public Column(String tableName, DataType dataType, String colName, boolean isNullable, boolean unique,
            boolean primaryKey, short ordinalPosition) {
        this.tableName = tableName;
        this.name = colName;
        this.type = dataType;
        this.ordinalPosition = ordinalPosition;
        this.notNull = isNullable;
        this.unique = unique;
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return String.format("(%s %s unique=%b, notNull=%b, primaryKey=%b, ordinalPosition=%d)", name, type, unique,
                notNull, primaryKey, ordinalPosition);
    }
}
