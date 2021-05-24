package dbProject;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class ValidateTableData {

	public int recordCount;
	public int rowIdCount;
	public ArrayList<TableLeafCell> columnData;
	private ArrayList<Column> columnNameAttrs;
	public String tableName;
	public boolean tableExists;
	public int rootPageNo;
	public int lastRowId;

	public ValidateTableData(String tableName) {
		this.tableName = tableName;
		tableExists = false;
		try {

			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(Utils.gettableFilePath(Settings.dbase_tab),
					"r");

			BPlusTree bplusTree = new BPlusTree(davisbaseTablesCatalog, tableName);
			for (Integer pageNo : bplusTree.getAllLeaves()) {
				Page page = new Page(davisbaseTablesCatalog, pageNo);
				for (Cell cell : page.getPageCells()) {
					TableLeafCell record = (TableLeafCell) cell;
					HashMap<Short, Attribute> attributes = record.getAttributes();
					if (attributes.get((short) 1).toString().equals(tableName)) {
						this.rootPageNo = Integer.parseInt(attributes.get((short) 4).toString());
						recordCount = Integer.parseInt(attributes.get((short) 2).toString());
						lastRowId = Integer.parseInt(attributes.get((short) 5).toString());
						tableExists = true;
						break;
					}
				}
				if (tableExists)
					break;
			}

			davisbaseTablesCatalog.close();
			if (tableExists) {
				loadColumnData();
			}

		} catch (IOException e) {
			System.out.println("! Error while checking Table " + tableName + " exists.");
			System.out.println(e);
		}
	}

	private void loadColumnData() {
		try {

			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
					Utils.gettableFilePath(Settings.dbase_coltable), "r");
			columnData = new ArrayList<>();
			columnNameAttrs = new ArrayList<>();
			BPlusTree bPlusTree = new BPlusTree(davisbaseColumnsCatalog, tableName);

			for (Integer pageNo : bPlusTree.getAllLeaves()) {

				Page page = new Page(davisbaseColumnsCatalog, pageNo);

				for (Cell cell : page.getPageCells()) {
					TableLeafCell record = (TableLeafCell) cell;
					HashMap<Short, Attribute> attributes = record.getAttributes();
					if (attributes.get((short) 1).toString().equals(tableName)) {
						columnData.add(record);
						Column colData = new Column(tableName,
								DataType.get(attributes.get((short) 3).fieldValuebyte[0]),
								attributes.get((short) 2).toString(), attributes.get((short) 5).toString().equals("NO"),
								attributes.get((short) 6).toString().equals("UNI"),
								attributes.get((short) 6).toString().equals("PRI"),
								Short.parseShort(attributes.get((short) 4).toString()));

						columnNameAttrs.add(colData);

					}
				}
			}

			davisbaseColumnsCatalog.close();
		} catch (IOException e) {
			System.out.println("! Error while getting column data for " + tableName);
			System.out.println(e);
		}

	}

	public boolean validateInsert(List<Attribute> row) throws IOException {
		RandomAccessFile tableFile = new RandomAccessFile(Utils.gettableFilePath(tableName), "r");
		DavisBaseFile file = new DavisBaseFile(tableFile);

		for (short i = 0; i < columnNameAttrs.size(); i++) {
			if (columnNameAttrs.get(i).unique || columnNameAttrs.get(i).primaryKey) {
				Condition condition = new Condition(columnNameAttrs.get(i).type, columnNameAttrs.get(i).name, "=",
						row.get(i));
				condition.columnOrdinal = i;
				if (file.recordExists(this, Arrays.asList(columnNameAttrs.get(i).name), condition)) {
					System.out.println("! Insert failed: Column " + columnNameAttrs.get(i).name + " should be unique.");
					tableFile.close();
					return false;
				}

			}

		}
		tableFile.close();
		return true;
	}

	public boolean columnExists(List<String> columns) {

		if (columns.size() == 0)
			return true;

		List<String> lColumns = new ArrayList<>(columns);

		for (Column column_name_attr : columnNameAttrs) {
			if (lColumns.contains(column_name_attr.name))
				lColumns.remove(column_name_attr.name);
		}

		return lColumns.isEmpty();
	}

	public Column getColumn(String columnName) {
		for (Column column : columnNameAttrs) {
			if (columnName.equals(column.name)) {
				return column;
			}
		}
		return null;
	}

	public Column getColumn(Short ordinalPosition) {
		for (Column column : columnNameAttrs) {
			if (column.ordinalPosition == ordinalPosition) {
				return column;
			}
		}
		return null;
	}

	public List<Column> getColumns() {
		return columnNameAttrs;
	}

	public List<String> getColumnNames() {
		List<String> columnNames = new ArrayList<String>();
		for (Column column : columnNameAttrs) {
			columnNames.add(column.name);
		}
		return columnNames;
	}

	public void updateMetaData(RandomAccessFile tableFile) {
		try {

			Integer rootPageNo = Utils.getRootPageNo(tableFile);
			// tableFile.close();

			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(Utils.gettableFilePath(Settings.dbase_tab),
					"rw");

			DavisBaseFile dbFile = new DavisBaseFile(davisbaseTablesCatalog);

			ValidateTableData validateTblData = new ValidateTableData("davisbase_tables");

			Condition condition = new Condition(DataType.TEXT, "table_name", "=",
					new Attribute(DataType.TEXT, tableName));
			condition.columnOrdinal = 1;

			List<String> columns = Arrays.asList("record_count", "root_page", "lastRowId");
			List<String> newValues = new ArrayList<>();
			Integer recCount = Integer.valueOf(recordCount);
			Integer rootPgNo = Integer.valueOf(rootPageNo);
			Integer lastRowId = Integer.valueOf(this.lastRowId);
			newValues.add(recCount.toString());
			newValues.add(rootPgNo.toString());
			newValues.add(lastRowId.toString());

			dbFile.updateRecords(validateTblData, condition, columns, newValues);

			davisbaseTablesCatalog.close();
		} catch (IOException e) {
			System.out.println("! Error updating meta data for " + tableName);
		}

	}

	public List<Short> getOrdinalPostions(List<String> columnNames) {
		List<Short> ordinalPostions = new ArrayList<>();
		for (String columnName : columnNames) {
			Column column = getColumn(columnName);
			ordinalPostions.add(column.ordinalPosition);
		}
		return ordinalPostions;
	}

}
