package dbProject;
public class Settings {
	static String prompt = "sql> ";
	static String version = "v1";
	static boolean isExit = false;
	/*
	 * Page size for all files is 512 bytes by default. You may choose to make it
	 * user modifiable
	 */
	static int pageSize = 512;
	public static String dbase_coltable = "davisbase_columns";
	public static String dbase_tab = "davisbase_tables";

	public static boolean isExit() {
		return isExit;
	}

	public static void setExit(boolean e) {
		isExit = e;
	}

	public static String getPrompt() {
		return prompt;
	}

	public static void setPrompt(String s) {
		prompt = s;
	}

	public static String getVersion() {
		return version;
	}

	public static void setVersion(String version) {
		Settings.version = version;
	}

	public static int getPageSize() {
		return pageSize;
	}

	public static void setPageSize(int pageSize) {
		Settings.pageSize = pageSize;
	}

	/**
	 * ***********************************************************************
	 * Static method definitions
	 */

	/**
	 * @param s   The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num
	 *         times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}
}
