CS-6360.501 Database Design Programming Project 2

Team Yellow -

Mathew Black - mrb160030
Srivastchavan Rengarajan - sxr190067
Yogesh Kumar Chandrasekar - yxc180071
Enrique Rodriguez Cupello - ejr170001
Parul Dumka - pxd200022

READ ME - 

SYSTEM CONFIGURATION:

OS: Windows 7/8/10 
Language: Java 
Version: JavaSE-11/12/13
IDE: VSCode/Eclipse

Project Goal - 

The goal of this project is to implement a rudimentary database engine that is based on a simplified file-per-table variation on the SQLite file format, which we call DavisBase, which operates entirely from the command line. Each file needs to be split into equal sized pages of size 512 Bytes. Data should be encoded into two types of files - table (.tbl) and index (.ndx) files.

Supported Commands - 
The database engine supports the following DDL, DML and DQL queries.

Show Tables 	- Show tables;
Create Table 	- Create Table newTable (col1 INT, col2 TEXT, col3 DATE);
Create Index 	- Create Index newTable col1;
Select Table 	- Select (col1,col2,col3) from newTable where col1 = 5;
Update Table 	- Update Table newTable Set col2 = "HelloWorld" WHERE col1 = 3;
Insert Record 	- Insert Into Table (col1, col2, col3) newTable values (7,"newRec",'2020-12-01');
Delete Table 	- Delete from table newTable where col1=6;
Drop Table 	- Drop table newTable;
Version		- version;
Help 		- help;
Exit 		- exit;


Design Assumptions -

Hidden unique rowid column is created for every table that is created using the application.
Only table constraints supported are PRIMARY KEY, NOT NULL and UNIQUE.
Insert command syntax - INSERT INTO TABLE (column_list) table_name VALUES (value_list);
Select query supports equalities, inequalities and negation.
Select query doesn't support nested queries, join conditions and complex WHERE conditions.
Update query for Text datatype supports only values with same length as the original text.
Date format - 'YYYY-MM-DD';
Time format - 'hh:mm:ss';
DateTime format - 'YYYY-MM-DD_hh:mm:ss'

Steps to Compile the project -

1. Install Java version 8 or above.
2. Install any IDE that supports Java, preferably Eclipse or VS Code.
3. Open the IDE and create new java project, module and package.
4. Copy & paste the source code from zip file folder 'src' into the newly created java project location ~/src/(package_name)/
5. Refresh the project to see all the files in the IDE.
6. Add the relevant Package (package_name); code on top of all the files.
7. Build and run the program once there is no error.
8. In the console, type in the any of the above listed commands to perform various operations in the database engine.
9. Type exit to exit the poject.

Project Output - 

Catalog files Location 		- ~/data/catalog/(davisbase_tables.tbl or davisbase_columns.tbl)
User created tables/indexes	- ~/data/user_data/(filename.tbl or filename.ndx)

Result - 

The database engine is successfully created and all the required goals are successfully met.
