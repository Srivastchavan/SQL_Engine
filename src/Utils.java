package dbProject;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class Utils {

  public static boolean dbInitialized = false;

  /**
   * Display the splash screen
   */
  public static void splashScreen() {
    System.out.println(printSeparator("-", 80));
    System.out.println("Welcome to DavisBaseLite"); // Display the string.
    System.out.println("DavisBaseLite Version " + Settings.getVersion());
    System.out.println("\nType \"help;\" to display supported commands.");
    System.out.println(printSeparator("-", 80));
  }

  public static String printSeparator(String s, int len) {
    String bar = "";
    for (int i = 0; i < len; i++) {
      bar += s;
    }
    return bar;
  }

  public static void deleteDir(File file) {
    File[] contents = file.listFiles();
    if (contents != null) {
      for (File f : contents) {
        deleteDir(f);
      }
    }
    file.delete();
  }

  public static void initializeDBStorage() {

    try {

      File data = new File("data");
      // DEBUG ONLY
      // deleteDir(data);

      data.mkdir();
      File catalog = new File("data/catalog");
      catalog.mkdir();

    } catch (SecurityException se) {
      System.out.println("Unable to create data container directory");
      System.out.println(se);
    }

    try {
      File userData = new File("data/user_data");
      userData.mkdir();
    } catch (SecurityException se) {
      System.out.println("Unable to create user data directory");
      System.out.println(se);
    }

    try {
      new TablesCatalog().init();

    } catch (IOException e) {
      System.out.println("Unable to intialize the database_tables file");
      System.out.println(e);
    }

    try {
      new ColumnsCatalog().init();
    } catch (IOException e) {
      System.out.println("Unable to intialize the database_columns file");
      System.out.println(e);
    }

    // DEBUG only
    // try {
    // new TablesCatalog().createTable("table2");
    // new ColumnsCatalog().addNewColumn(new Column("table2", DataType.INT, "test1",
    // false, false, false, (short) 1));
    // RandomAccessFile indexFile = new
    // RandomAccessFile("data/user_data/table2_test1.ndx", "rw");
    // new Page(indexFile, PageType.LEAFINDEX, -1, -1);
    // BTree test = new BTree("table2", "test1");
    // for (int i = 0; i < 100; i++) {
    // Attribute value = new Attribute(DataType.INT, Integer.toString(i % 16));
    // test.insertValue(value, i);
    // Page rootPage = test.getRootPage();
    // List<Page> children = rootPage.getChildPages();
    // if (i > 74) {
    // System.out.println(i / 16);
    // System.out.println(children);
    // }
    // }
    // } catch (IOException e) {
    // System.out.println("Unable to intialize the database_columns file");
    // System.out.println(e);
    // }

  }

  public static int getRootPageNo(RandomAccessFile binaryfile) {
    int rootpage = 0;
    try {
      for (int i = 0; i < binaryfile.length() / Settings.pageSize; i++) {
        binaryfile.seek(i * Settings.pageSize + 0x0A);
        int a = binaryfile.readInt();
        if (a == -1) {
          return i;
        }
      }
      return rootpage;
    } catch (IOException e) {
      System.out.println("error while getting root page no ");
      System.out.println(e);
    }
    return -1;

  }

  public static int indexOfIgnoreCase(ArrayList<String> tokens, String compareString) {
    for (int i = 0; i < tokens.size(); i++) {
      if (compareString.equalsIgnoreCase(tokens.get(i))) {
        return i;
      }
    }
    return -1;
  }

  public static boolean containsIgnoreCase(ArrayList<String> tokens, String compareString) {
    return indexOfIgnoreCase(tokens, compareString) != -1;
  }

  public static Byte[] byteToBytes(byte[] data) {
    int length = data == null ? 0 : data.length;
    Byte[] result = new Byte[length];
    for (int i = 0; i < length; i++)
      result[i] = data[i];
    return result;
  }

  public static byte[] Bytestobytes(final Byte[] data) {

    if (data == null) {
      return null;
    }
    int length = data == null ? 0 : data.length;
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++)
      result[i] = data[i];
    return result;
  }

  public static String gettableFilePath(String tableName) {
    if (tableName.contentEquals(Settings.dbase_tab) || tableName.equals(Settings.dbase_coltable)) {
      return "data/catalog/" + tableName + ".tbl";
    } else {
      return "data/user_data/" + tableName + ".tbl";
    }
  }
}
