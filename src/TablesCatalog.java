package dbProject;
import java.io.IOException;
//import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
//import java.util.HashMap;

public class TablesCatalog {

    TablesCatalog() {

    }

    public void createTable(String tableName) throws IOException {
        RandomAccessFile file = new RandomAccessFile(Utils.gettableFilePath(Settings.dbase_tab), "rw");
        BPlusTree bPlusTree = new BPlusTree(file, Settings.dbase_tab);
        Page page = bPlusTree.getPageForInsert();
        ValidateTableData metadata = new ValidateTableData(Settings.dbase_tab);
        metadata.lastRowId++;
        metadata.recordCount++;
        try {
            page.addCell(new TableLeafCell(metadata.lastRowId,
                    Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, tableName),
                            new Attribute(DataType.INT, "0"), new Attribute(DataType.SMALLINT, "0"),
                            new Attribute(DataType.SMALLINT, "0"), new Attribute(DataType.INT, "0") })));

        } catch (PageOverflowException pageException) {
            bPlusTree.handlePageOverflow(pageException.page, pageException.newCell);
        }
        metadata.updateMetaData(file);
        file.close();
    }

    public void init() throws IOException {
        RandomAccessFile file = new RandomAccessFile(Utils.gettableFilePath(Settings.dbase_tab), "rw");
        if (file.length() == 0) {
            BPlusTree bPlusTree = new BPlusTree(file, Settings.dbase_tab);
            Page page = new Page(file, PageType.LEAF, -1, -1);

            try {
                page.addCell(new TableLeafCell(1,
                        Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, Settings.dbase_tab),
                                new Attribute(DataType.INT, "2"), new Attribute(DataType.SMALLINT, "0"),
                                new Attribute(DataType.SMALLINT, "0"), new Attribute(DataType.INT, "2") })));

                page.addCell(new TableLeafCell(2,
                        Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, Settings.dbase_coltable),
                                new Attribute(DataType.INT, "11"), new Attribute(DataType.SMALLINT, "0"),
                                new Attribute(DataType.SMALLINT, "0"), new Attribute(DataType.INT, "11") })));
            } catch (PageOverflowException e) {
                bPlusTree.handlePageOverflow(e.page, e.newCell);
                System.out.println("Page overflow during table creation");

            }
        }
        file.close();
    }
}
