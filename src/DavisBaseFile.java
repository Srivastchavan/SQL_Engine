package dbProject;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DavisBaseFile {

    public static boolean showRowId = false;

    static int pageSizeExponent = 9;
    static int pageSize = (int) Math.pow(2, pageSizeExponent);

    RandomAccessFile dbFile;

    public DavisBaseFile(RandomAccessFile file) {
        this.dbFile = file;
    }

    public int updateRecords(ValidateTableData validateTblData, Condition condition, List<String> columnNames,
            List<String> newValues) throws IOException {
        List<Integer> updatedRowIds = new ArrayList<Integer>();
        HashMap<Integer, HashMap<Short, Attribute>> updatedAttributes = new HashMap<>();

        int k = 0;
        HashMap<Short, Attribute> newValueMap = new HashMap<>();

        for (String columnName : columnNames) {
            Column column = validateTblData.getColumn(columnName);
            try {
                newValueMap.put(column.ordinalPosition, new Attribute(column.type, newValues.get(k)));
            } catch (Exception e) {
                System.out.println("! Invalid data format for column " + column.name);
                return 0;
            }
            k++;
        }

        BPlusTree bPlusTree = new BPlusTree(dbFile, validateTblData.tableName);
        List<Integer> pages = bPlusTree.getAllLeaves(condition);
        if (condition != null) {
            BTree indexTree = new BTree(validateTblData.tableName, condition.columnName);
            if (indexTree.exists) {
                List<Integer> rowIds = indexTree.findRowIds(condition);
                pages = new ArrayList<Integer>(bPlusTree.getPageNumbers(rowIds));
            }
        }

        for (Integer pageNo : pages) {
            Page page = new Page(dbFile, pageNo);
            for (Cell cell : page.getPageCells()) {
                TableLeafCell record = (TableLeafCell) cell;
                if (condition != null) {
                    if (!condition.evaluate(record.getAttributes().get(condition.columnOrdinal)))
                        continue;
                }
                updatedRowIds.add(record.rowId);
                HashMap<Short, Attribute> attributes = record.getAttributes();
                updatedAttributes.put(record.rowId, attributes);
                for (short i : newValueMap.keySet()) {
                    if ((attributes.get(i).dataType == DataType.TEXT
                            && attributes.get(i).toString().length() == newValueMap.get(i).toString().length())
                            || (attributes.get(i).dataType != DataType.NULL
                                    && attributes.get(i).dataType != DataType.TEXT)) {
                        record.update(page.file, newValueMap.get(i).fieldValueByte, i);
                    } else {
                        System.out.println(
                                "New string attribute is not the same as the original assigned length this is not supported!");
                    }
                }
            }
        }

        for (String columnName : columnNames) {
            Column column = validateTblData.getColumn(columnName);
            BTree index = new BTree(validateTblData.tableName, column.name);
            if (index.exists && newValueMap.containsKey(column.ordinalPosition)) {
                Attribute newValue = newValueMap.get(column.ordinalPosition);
                for (int rowId : updatedRowIds) {
                    index.updateRowId(rowId, updatedAttributes.get(rowId).get(column.ordinalPosition), newValue);
                }
            }
        }

        if (!validateTblData.tableName.equals("davisbase_tables")
                && !validateTblData.tableName.equals("davisbase_columns"))
            System.out.println("* " + updatedRowIds.size() + " record(s) updated.");

        return updatedRowIds.size();

    }

    public void selectRecords(ValidateTableData validateTblData, List<String> columNames, Condition condition)
            throws IOException {

        // The select order might be different from the table ordinal position
        List<Short> ordinalPostions = validateTblData.getOrdinalPostions(columNames);

        System.out.println();

        ArrayList<Integer> printPosition = new ArrayList<>();

        int columnPrintLength = 0;
        printPosition.add(columnPrintLength);
        int totalTablePrintLength = 0;
        if (showRowId) {
            System.out.print("rowid");
            System.out.print(Utils.printSeparator(" ", 5));
            printPosition.add(10);
            totalTablePrintLength += 10;
        }

        for (short i : ordinalPostions) {
            String columnName = validateTblData.getColumn(i).name;
            columnPrintLength = Math.max(columnName.length(), validateTblData.getColumn(i).type.getPrintOffset()) + 5;
            printPosition.add(columnPrintLength);
            System.out.print(columnName);
            System.out.print(Utils.printSeparator(" ", columnPrintLength - columnName.length()));
            totalTablePrintLength += columnPrintLength;
        }
        System.out.println();
        System.out.println(Utils.printSeparator("-", totalTablePrintLength));

        BPlusTree bPlusTree = new BPlusTree(dbFile, validateTblData.tableName);

        List<Integer> pages = bPlusTree.getAllLeaves(condition);
        if (condition != null) {
            BTree indexTree = new BTree(validateTblData.tableName, condition.columnName);
            if (indexTree.exists) {
                List<Integer> rowIds = indexTree.findRowIds(condition);
                pages = new ArrayList<Integer>(bPlusTree.getPageNumbers(rowIds));
            }
        }
        String tableNameCheck = validateTblData.tableName;
        DataType newVal = null;
        String currentValue = "";
        for (Integer pageNo : pages) {
            Page page = new Page(dbFile, pageNo);
            for (Cell cell : page.getPageCells()) {
                TableLeafCell record = (TableLeafCell) cell;
                if (condition != null) {
                    if (!condition.evaluate(record.getAttributes().get(condition.columnOrdinal)))
                        continue;
                }
                int columnCount = 0;
                if (showRowId) {
                    currentValue = Integer.valueOf(record.rowId).toString();
                    System.out.print(currentValue);
                    System.out
                            .print(Utils.printSeparator(" ", printPosition.get(++columnCount) - currentValue.length()));
                }
                for (short i : ordinalPostions) {
                    currentValue = record.getAttributes().get(i).toString();
                    // System.out.print(record.getAttributes().get(i));
                    if (i == 3 && tableNameCheck.contentEquals("davisbase_columns")) {
                        newVal = DataType.get(record.getAttributes().get(i).fieldValuebyte[0]);
                        currentValue = newVal.toString();
                    }
                    System.out.print(currentValue);
                    System.out
                            .print(Utils.printSeparator(" ", printPosition.get(++columnCount) - currentValue.length()));
                }
                System.out.println();
            }
        }

        System.out.println();

    }

    public boolean recordExists(ValidateTableData validateTblData, List<String> columNames, Condition condition)
            throws IOException {

        BPlusTree bPlusTree = new BPlusTree(dbFile, validateTblData.tableName);

        for (Integer pageNo : bPlusTree.getAllLeaves(condition)) {
            Page page = new Page(dbFile, pageNo);
            for (Cell cell : page.getPageCells()) {
                TableLeafCell record = (TableLeafCell) cell;
                if (condition != null) {
                    if (!condition.evaluate(record.getAttributes().get(condition.columnOrdinal)))
                        continue;
                }
                return true;
            }
        }
        return false;

    }

}