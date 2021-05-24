package dbProject;
import static java.lang.System.out;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Commands {

	/*
	 * This method determines what type of command the userCommand is and calls the
	 * appropriate method to parse the userCommand String.
	 */
	public static void parseUserCommand(String userCommand) throws Exception {

		/*
		 * commandTokens is an array of Strings that contains one lexical token per
		 * array element. The first token can be used to determine the type of command
		 * The other tokens can be used to pass relevant parameters to each
		 * command-specific method inside each case statement
		 */
		ArrayList<String> commandTokens = commandStringToTokenList(userCommand);

		/*
		 * This switch handles a very small list of hard-coded commands from SQL syntax.
		 * You will want to rewrite this method to interpret more complex commands.
		 */
		switch (commandTokens.get(0).toLowerCase()) {
			case "show":
				System.out.println("Case: SHOW");
				show(commandTokens);
				break;
			case "select":
				System.out.println("Case: SELECT");
				parseQuery(commandTokens);
				break;
			case "create":
				System.out.println("Case: CREATE");
				if (commandTokens.get(1).equalsIgnoreCase("table")) {
					parseCreateTable(userCommand);
				} else if (commandTokens.get(1).equalsIgnoreCase("index")) {
					parseCreateIndex(userCommand);
				} else {
					out.println("Unknown creation type: " + commandTokens.get(1));
				}

				break;
			case "insert":
				System.out.println("Case: INSERT");
				parseInsert(commandTokens);
				break;
			case "delete":
				System.out.println("Case: DELETE");
				parseDelete(commandTokens);
				break;
			case "update":
				System.out.println("Case: UPDATE");
				parseUpdate(commandTokens);
				break;
			case "drop":
				System.out.println("Case: DROP");
				dropTable(commandTokens);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				Settings.setExit(true);
				break;
			case "quit":
				Settings.setExit(true);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + Settings.getVersion());
	}

	public static void parseCreateTable(String command) {

		System.out.println("Stub: parseCreateTable method");
		System.out.println("Command: " + command);
		ArrayList<String> commandTokens = commandStringToTokenList(command);
		System.out.println(commandTokens);
		/* Extract the table name from the command string token list */
		String tableName = commandTokens.get(2);

		ArrayList<String> columnTuples = parseTuple(commandTokens.get(3));
		System.out.println(columnTuples);

		ArrayList<Column> columns = new ArrayList<Column>();
		short ordinalPosition = 1;
		for (String columnString : columnTuples) {
			columns.add(new Column(tableName, columnString, ordinalPosition));
			ordinalPosition++;
		}
		if (columns.size() == 0) {
			out.println("No columns specified!");
			return;
		}
		/* Code to create a .tbl file to contain table data */
		try {
			ValidateTableData metadata = new ValidateTableData(tableName);

			if (metadata.tableExists) {
				System.out.println("! Duplicate table Name");
				return;
			}
			TablesCatalog tablesCatalog = new TablesCatalog();

			tablesCatalog.createTable(tableName);

			RandomAccessFile tableFile = new RandomAccessFile(Utils.gettableFilePath(tableName), "rw");
			new Page(tableFile, PageType.LEAF, -1, -1);

			ColumnsCatalog columnsCatalog = new ColumnsCatalog();

			for (Column column : columns) {
				columnsCatalog.addNewColumn(column);
			}

			System.out.println("* Table created");
		} catch (IOException e) {
			System.out.println("Error during table creation");
			System.out.println(e);
		}
	}

	public static void parseCreateIndex(String command) {
		ArrayList<String> commandTokens = commandStringToTokenList(command);
		/* Extract the table name from the command string token list */
		String tableName = commandTokens.get(2);

		ArrayList<String> columnTuples = parseTuple(commandTokens.get(3));

		String columnName = columnTuples.get(0);

		try {
			ValidateTableData metadata = new ValidateTableData(tableName);
			Column column = metadata.getColumn(columnName);

			if (column == null) {
				System.out.println("Column " + columnName + " does not exist for table " + tableName);
				return;
			}

			if (new File("data/user_data/" + tableName + "_" + columnName + ".ndx").exists()) {
				System.out.println("Column index " + columnName + " for table " + tableName + " already exists");
				return;
			}

			RandomAccessFile indexFile = new RandomAccessFile("data/user_data/" + tableName + "_" + columnName + ".ndx",
					"rw");
			new Page(indexFile, PageType.LEAFINDEX, -1, -1);

			RandomAccessFile dbFile = new RandomAccessFile(Utils.gettableFilePath(tableName), "rw");
			BPlusTree bPlusTree = new BPlusTree(dbFile, tableName);
			BTree index = new BTree(tableName, column.name);
			// iterate over all existing values in the table when adding a new index to
			// build it
			for (Integer pageNo : bPlusTree.getAllLeaves(null)) {
				Page page = new Page(dbFile, pageNo);
				for (Cell cell : page.getPageCells()) {
					TableLeafCell record = (TableLeafCell) cell;
					Attribute value = record.getAttributes().get(column.ordinalPosition);
					index.insertValue(value, record.rowId);
				}
			}

		} catch (IOException e) {
			System.out.println("Error during index creation");
			System.out.println(e);
		}
	}

	public static void show(ArrayList<String> commandTokens) throws Exception {
		if (commandTokens.get(1).equalsIgnoreCase("TABLES")) {
			parseUserCommand("select * from davisbase_tables");
		}
	}

	/*
	 * Stub method for inserting a new record into a table.
	 */
	public static void parseInsert(ArrayList<String> commandTokens) {
		try {
			if (!commandTokens.get(1).equalsIgnoreCase("INTO") || !commandTokens.get(2).equalsIgnoreCase("TABLE")
					|| Utils.indexOfIgnoreCase(commandTokens, "VALUES") == -1) {
				System.out.println("Error with insert command syntax please try again");
				return;
			}
			ArrayList<String> columns = parseTuple(commandTokens.get(3));
			/* Extract the table name from the command string token list */
			String tableName = commandTokens.get(4);
			ArrayList<String> values = parseTuple(commandTokens.get(6));
			ValidateTableData validateTblData = new ValidateTableData(tableName);

			if (!validateTblData.tableExists) {
				System.out.println("! Table does not exist.");
				return;
			}
			for (String colToken : columns) {
				if (validateTblData.getColumn(colToken.trim()) == null) {
					System.out.println("! Invalid column : " + colToken.trim());
					return;
				}
			}

			ArrayList<Attribute> attributeToInsert = new ArrayList<>();

			for (Column col : validateTblData.getColumns()) {
				int i = 0;
				boolean columnProvided = false;
				for (i = 0; i < columns.size(); i++) {
					if (columns.get(i).trim().equals(col.name)) {
						columnProvided = true;
						try {
							String value = values.get(i).replace("'", "").replace("\"", "").trim();
							if (values.get(i).trim().equals("null")) {
								if (col.notNull) {
									System.out.println("! Cannot Insert NULL into " + col.name);
									return;
								}
								col.type = DataType.NULL;
								value = value.toUpperCase();
							}
							Attribute attr = new Attribute(col.type, value);
							attributeToInsert.add(attr);
							break;
						} catch (Exception e) {
							System.out.println(
									"! Invalid data format for " + columns.get(i) + " values: " + values.get(i));
							return;
						}
					}
				}
				if (columns.size() > i) {
					columns.remove(i);
					values.remove(i);
				}

				if (!columnProvided) {
					if (!col.notNull)
						attributeToInsert.add(new Attribute(DataType.NULL, "NULL"));
					else {
						System.out.println("! Cannot Insert NULL into " + col.name);
						return;
					}
				}
			}

			// insert attributes to the page
			RandomAccessFile dstTable = new RandomAccessFile(Utils.gettableFilePath(tableName), "rw");

			BPlusTree bPlusTree = new BPlusTree(dstTable, tableName);
			Page dstPage = bPlusTree.getPageForInsert();
			validateTblData.lastRowId++;
			validateTblData.recordCount++;
			try {
				if (dstPage.addCell(new TableLeafCell(validateTblData.lastRowId, attributeToInsert)) != null)
					System.out.println("* Record Inserted");
			} catch (PageOverflowException pageException) {
				bPlusTree.handlePageOverflow(pageException.page, pageException.newCell);
			}

			validateTblData.updateMetaData(dstTable);
			dstTable.close();
			int i = 0;
			for (Column column : validateTblData.getColumns()) {
				Attribute value = attributeToInsert.get(i);
				BTree index = new BTree(tableName, column.name);
				if (index.exists) {
					index.insertValue(value, validateTblData.lastRowId);
				}
				i++;
			}

		} catch (IOException ex) {
			System.out.println("! Error while inserting record");
			System.out.println(ex);

		}

	}

	public static void parseDelete(ArrayList<String> commandTokens) {
		/* Extract the table name from the command string token list */

		try {
			if (!commandTokens.get(1).equalsIgnoreCase("FROM") || !commandTokens.get(2).equalsIgnoreCase("TABLE")) {
				System.out.println("! Syntax Error");
				return;
			}
			String tableName = commandTokens.get(3);
			String tableFilePath = Utils.gettableFilePath(tableName);

			ValidateTableData metadata = new ValidateTableData(tableName);
			Condition filter = null;
			try {
				filter = parseCondition(metadata, commandTokens);
			} catch (Exception e) {
				System.out.println(e);
				return;
			}

			HashMap<Integer, HashMap<Short, Attribute>> deletedAttributes = new HashMap<>();

			RandomAccessFile tableFile = new RandomAccessFile(tableFilePath, "rw");

			BPlusTree tree = new BPlusTree(tableFile, metadata.tableName);
			List<Integer> pages = tree.getAllLeaves(filter);
			if (filter != null) {
				BTree indexTree = new BTree(metadata.tableName, filter.columnName);
				if (indexTree.exists) {
					List<Integer> rowIds = indexTree.findRowIds(filter);
					pages = new ArrayList<Integer>(tree.getPageNumbers(rowIds));
				}
			}
			List<Integer> deletedRowIds = new ArrayList<Integer>();
			for (int pageNo : pages) {
				short deleteCountPerPage = 0;
				Page page = new Page(tableFile, pageNo);
				List<Cell> currentCells = new ArrayList<Cell>(page.getPageCells());
				for (Cell cell : currentCells) {
					TableLeafCell record = (TableLeafCell) cell;
					if (filter != null) {
						if (!filter.evaluate(record.getAttributes().get(filter.columnOrdinal)))
							continue;
					}
					deletedAttributes.put(record.rowId, record.getAttributes());
					metadata.recordCount--;
					page.deleteCell(Integer.valueOf(record.cellIndex - deleteCountPerPage).shortValue());
					deleteCountPerPage++;
					deletedRowIds.add(record.rowId);
				}
			}

			for (Column column : metadata.getColumns()) {
				BTree index = new BTree(metadata.tableName, column.name);
				if (index.exists) {
					for (Integer rowId : deletedRowIds) {
						index.deleteRowId(rowId, deletedAttributes.get(rowId).get(column.ordinalPosition));
					}
				}
			}

			metadata.updateMetaData(tableFile);
			tableFile.close();
			System.out.println(deletedRowIds.size() + " record(s) deleted!");
		} catch (IOException e) {
			System.out.println("! Error on deleting rows");
			System.out.println(e.getMessage());
		}

	}

	/**
	 * Stub method for dropping tables
	 * 
	 * @throws Exception
	 */
	public static void dropTable(ArrayList<String> commandTokens) throws Exception {
		String tableName = commandTokens.get(2);

		parseUserCommand("DELETE from table " + Settings.dbase_tab + " where table_name = '" + tableName + "' ");
		parseUserCommand("DELETE from table " + Settings.dbase_coltable + " where table_name = '" + tableName + "' ");
		File tableFile = new File(Utils.gettableFilePath(tableName));
		if (tableFile.delete()) {
			System.out.println("table deleted");
		} else {
			System.out.println("table doesn't exist");
		}

		Path user_data_path = FileSystems.getDefault().getPath("data", "user_data");
		try (DirectoryStream<Path> paths = Files.newDirectoryStream(user_data_path, tableName + "*.ndx")) {
			for (Path path : paths) {
				path.toFile().delete();
			}
		}

	}

	/**
	 * Stub method for executing queries
	 */
	public static void parseQuery(ArrayList<String> commandTokens) {
		if (!Utils.containsIgnoreCase(commandTokens, "FROM")) {
			System.out.println("Improper syntax please try again");
			return;
		}

		ArrayList<String> columns = new ArrayList<String>(
				commandTokens.subList(1, Utils.indexOfIgnoreCase(commandTokens, "FROM")));

		String tableName = commandTokens.get(Utils.indexOfIgnoreCase(commandTokens, "FROM") + 1);

		ValidateTableData validateTblData = new ValidateTableData(tableName);
		String tableFilePath = Utils.gettableFilePath(tableName);
		if (!validateTblData.tableExists) {
			System.out.println("! Table does not exist");
			return;
		}

		// TODO: Check to see if this is optional or not
		// kinda up to use but the docs seem to want it to be required but idc
		Condition filter = null;
		try {
			filter = parseCondition(validateTblData, commandTokens);

		} catch (Exception e) {
			System.out.println(e);
			return;
		}

		// Might want to handle the wildcard case differently but we could always
		// just check if we have a *.
		if (columns.contains("*")) {
			columns = new ArrayList<String>(validateTblData.getColumnNames());
		}
		try {
			RandomAccessFile tableFile = new RandomAccessFile(tableFilePath, "r");
			DavisBaseFile select = new DavisBaseFile(tableFile);
			select.selectRecords(validateTblData, columns, filter);
			tableFile.close();
		} catch (IOException exception) {
			System.out.println("! Error in selecting columns from table");
		}
	}

	/**
	 * Stub method for updating records
	 * 
	 * @param updateString is a String of the user input
	 * @throws Exception
	 */
	public static void parseUpdate(ArrayList<String> commandTokens) throws Exception {

		/* Extract the table name from the command string token list */
		String tableName = commandTokens.get(1);
		String tableFilePath = Utils.gettableFilePath(tableName);
		int wherePosition = Utils.indexOfIgnoreCase(commandTokens, "WHERE");
		if (wherePosition == -1 || !commandTokens.get(2).equalsIgnoreCase("SET")) {
			System.out.println("Improper syntax please try again");
			return;
		}
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		for (int i = 3; i < wherePosition; i += 3) {
			if (commandTokens.get(i + 1).equals("=")) {
				columns.add(commandTokens.get(i));
				values.add(commandTokens.get(i + 2));
			}
		}
		ValidateTableData validateTBLData = new ValidateTableData(tableName);
		if (!validateTBLData.tableExists) {
			System.out.println("! Invalid Table name");
			return;
		}

		if (!validateTBLData.columnExists(columns)) {
			System.out.println("! Invalid column name(s)");
			return;
		}

		Condition filter = null;
		try {
			filter = parseCondition(validateTBLData, commandTokens);

		} catch (Exception e) {
			System.out.println(e);
			return;
		}
		// No where condition in update so exit
		if (filter == null) {
			return;
		}
		try {
			RandomAccessFile file = new RandomAccessFile(tableFilePath, "rw");
			DavisBaseFile D_File = new DavisBaseFile(file);
			int noOfRecordsupdated = D_File.updateRecords(validateTBLData, filter, columns, values);

			if (noOfRecordsupdated > 0) {
				List<Integer> allRowids = new ArrayList<>();
				for (Column colInfo : validateTBLData.getColumns()) {
					for (int i = 0; i < columns.size(); i++)
						if (colInfo.name.equals(columns.get(i))) // check if index exists
						{

							if (filter == null) {

								if (allRowids.size() == 0) {
									BPlusTree Tree = new BPlusTree(file, validateTBLData.tableName);
									for (int pageNo : Tree.getAllLeaves()) {
										Page currentPage = new Page(file, pageNo);
										for (Cell cell : currentPage.getPageCells()) {
											TableLeafCell record = (TableLeafCell) cell;
											allRowids.add(record.rowId);
										}
									}
								}
							}
						}
				}
			}

			file.close();

		} catch (IOException e) {
			out.println("Unable to update the table: " + tableName);
			out.println(e);

		}

	}

	public static String tokensToCommandString(ArrayList<String> commandTokens) {
		String commandString = "";
		for (String token : commandTokens)
			commandString = commandString + token + " ";
		return commandString;
	}

	public static ArrayList<String> commandStringToTokenList(String command) {
		command = command.replace(",", " , ");
		command = command.replaceAll("([=><!]+)", " $1 ");
		command = command.replaceAll("\\s+", " ");
		Pattern r = Pattern.compile("(?:[\\w=><*!\\-:+._]+|\\([^)]+\\))");
		Matcher matcher = r.matcher(command);
		ArrayList<String> tokenizedCommand = new ArrayList<String>();
		while (matcher.find()) {
			tokenizedCommand.add(matcher.group(0));
		}

		return tokenizedCommand;
	}

	public static ArrayList<String> parseTuple(String tuple) {
		tuple = tuple.replace("(", "");
		tuple = tuple.replace(")", "");
		tuple = tuple.replace(" , ", ",");
		return new ArrayList<String>(Arrays.asList(tuple.split(",")));
	}

	public static Condition parseCondition(ValidateTableData validateTblData, ArrayList<String> conditionTokens)
			throws Exception {

		if (Utils.containsIgnoreCase(conditionTokens, "WHERE")) {
			ArrayList<String> whereClause = new ArrayList<String>(conditionTokens
					.subList(Utils.indexOfIgnoreCase(conditionTokens, "WHERE") + 1, conditionTokens.size()));

			return new Condition(whereClause, validateTblData);
		} else {
			return null;
		}
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		out.println(Utils.printSeparator("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("SELECT ⟨column_list⟩ FROM table_name [WHERE condition];\n");
		out.println("\tDisplay table records whose optional condition");
		out.println("\tis <column_name> = <value>.\n");
		out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
		out.println("\tInsert new record into the table.\n");
		out.println(
				"CREATE TABLE table_name (\n\tcolumn_name1 data_type1 [PRIMARY KEY][NOT NULL][UNIQUE],\n\tcolumn_name2 data_type2 [PRIMARY KEY][NOT NULL][UNIQUE],\n\t...\n);");
		out.println("\tCreate new table with columns\n");
		out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("DROP TABLE table_name;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(Utils.printSeparator("*", 80));
	}

}
