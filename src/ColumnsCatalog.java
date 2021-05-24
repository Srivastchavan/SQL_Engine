package dbProject;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class ColumnsCatalog {

        ColumnsCatalog() {

        }

        public void addNewColumn(Column coldata) throws IOException {
                RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                ValidateTableData metadata = new ValidateTableData(Settings.dbase_coltable);
                metadata.lastRowId++;
                metadata.recordCount++;
                metadata.updateMetaData(file);
                file.close();
                addNewColumn(coldata, metadata.lastRowId);
        }

        public void addNewColumn(Column coldata, int rowId) throws IOException {
                RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                BPlusTree bPlusTree = new BPlusTree(file, Settings.dbase_coltable);
                Page dstPage = bPlusTree.getPageForInsert();
                try {
                        Attribute columnKey = new Attribute(DataType.NULL, "NULL");
                        if (coldata.primaryKey) {
                                columnKey = new Attribute(DataType.TEXT, "PRI");
                        } else if (coldata.unique) {
                                columnKey = new Attribute(DataType.TEXT, "UNI");
                        }
                        dstPage.addCell(new TableLeafCell(rowId, Arrays.asList(new Attribute[] {
                                        new Attribute(DataType.TEXT, coldata.tableName),
                                        new Attribute(DataType.TEXT, coldata.name),
                                        new Attribute(DataType.TINYINT, new byte[] { coldata.type.getValue() }),
                                        new Attribute(DataType.SMALLINT, coldata.ordinalPosition.toString()),
                                        new Attribute(DataType.TEXT, coldata.notNull ? "NO" : "YES"), columnKey })));

                } catch (PageOverflowException pageException) {
                        bPlusTree.handlePageOverflow(pageException.page, pageException.newCell);
                }
                file.close();
        }

        public void init() throws IOException {
                RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                if (file.length() == 0) {
                        new Page(file, PageType.LEAF, -1, -1);
                        short ordinal_position = 1;

                        addNewColumn(new Column(Settings.dbase_tab, DataType.TEXT, "table_name", true, false, false,
                                        ordinal_position++), 1);
                        addNewColumn(new Column(Settings.dbase_tab, DataType.INT, "record_count", true, false, false,
                                        ordinal_position++), 2);
                        addNewColumn(new Column(Settings.dbase_tab, DataType.SMALLINT, "avg_length", true, false, false,
                                        ordinal_position++), 3);
                        addNewColumn(new Column(Settings.dbase_tab, DataType.SMALLINT, "root_page", true, false, false,
                                        ordinal_position++), 4);
                        addNewColumn(new Column(Settings.dbase_tab, DataType.INT, "lastRowId", true, false, false,
                                        ordinal_position++), 5);

                        ordinal_position = 1;

                        addNewColumn(new Column(Settings.dbase_coltable, DataType.TEXT, "table_name", true, false,
                                        false, ordinal_position++), 6);
                        addNewColumn(new Column(Settings.dbase_coltable, DataType.TEXT, "column_name", true, false,
                                        false, ordinal_position++), 7);
                        addNewColumn(new Column(Settings.dbase_coltable, DataType.SMALLINT, "data_type", true, false,
                                        false, ordinal_position++), 8);
                        addNewColumn(new Column(Settings.dbase_coltable, DataType.SMALLINT, "ordinal_position", true,
                                        false, false, ordinal_position++), 9);
                        addNewColumn(new Column(Settings.dbase_coltable, DataType.TEXT, "is_nullable", true, false,
                                        false, ordinal_position++), 10);
                        addNewColumn(new Column(Settings.dbase_coltable, DataType.SMALLINT, "column_key", false, false,
                                        false, ordinal_position++), 11);
                }
                file.close();

        }
}