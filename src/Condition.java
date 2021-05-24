package dbProject;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Arrays;

public class Condition {
    boolean inverted = false;
    String columnName;
    String condition;
    Attribute value;

    public short columnOrdinal;
    public DataType type;
    boolean negation;

    Condition(ArrayList<String> tokens, ValidateTableData metadata) throws Exception {
        inverted = tokens.get(0).equalsIgnoreCase("NOT");
        int start = inverted ? 1 : 0;
        columnName = tokens.get(start);
        condition = tokens.get(start + 1);
        if (metadata.tableExists && metadata.columnExists(new ArrayList<String>(Arrays.asList(columnName)))) {
            columnOrdinal = metadata.getColumn(columnName).ordinalPosition;
            type = metadata.getColumn(columnName).type;
        } else {
            throw new Exception("! Invalid Table and Column : " + metadata.tableName + " . " + columnName);
        }
        value = new Attribute(type, tokens.get(start + 2)); // need to figure out how to get this to be the correct type
    }

    Condition(DataType type, String columnName, String condition, Attribute value) {
        this.type = type;
        this.columnName = columnName;
        this.condition = condition;
        this.value = value;
    }

    Condition(Cell cell, String condition) {
        this.type = cell.value.dataType;
        this.value = cell.value;
        this.condition = condition;
    }

    Condition(Attribute value, String condition) {
        this.type = value.dataType;
        this.value = value;
        this.condition = condition;
    }

    public boolean evaluate(Attribute compareValue) {
        boolean result;
        int comparison = type.compare(compareValue, value);
        switch (condition) {
            case "=":
                result = comparison == 0;
                break;
            case "<":
                result = comparison < 0;
                break;
            case ">":
                result = comparison > 0;
                break;
            case ">=":
                result = comparison >= 0;
                break;
            case "<=":
                result = comparison <= 0;
                break;
            case "<>":
            case "!=":
                result = comparison != 0;
                break;
            default:
                return false;
        }
        return inverted ? !result : result;
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s, inverted=%b)", columnName, condition, value, inverted);
    }

    public void setNegation(boolean negate) {
        this.negation = negate;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

}
