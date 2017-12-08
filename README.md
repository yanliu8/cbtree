# Counted B tree index access method on PostgreSQL

This extension allow user to create a index build with counted B tree.

# Supported functionality

1. Build
	Build a cbtree index on a existing table. The index will build the counted B tree from the default order of the heap table.
2. Search
	Search for a tuple at specified location in sequence. 
	Note: Only equality search is supprted now.
3. Insert
	Insert new tuples into the index as user insert new tuple into heap table.

# How to use it
1. Copy this directory to contrib/ directory under source code and add cbtree to the contrib Makefile. Make and install the whole postgres source code.

OR
	Copy the cbtree.so file to lib directory under the compiled postgres code directory.
	Copy cbtree.control, cbtree--1.0.sql to share/extension directory under the compiled postgres code directory.

2. Import cbtree into postgres by running this command in postgres client console.
	CREATE EXTENSION cbtree;

3. Create a dummy column with type int and build the index on it.
	CREATE TABLE demo (data_col int, dummy_col int);
	CREATE INDEX ON demo USING cbtree (dummy_col);

4. To insert new tuple into the table, specify the position you want to insert this tuple into the counted B tree in this dummy column. The position should be an unsigned integer >= 1. If position is greater than the total number of tuples in the counted B tree, the tuple will be inserted to the back of the sequence.
	INSERT INTO demo VALUES(10, 1);

5. To search for a tuple at certain position, run a select where command. Only equal operator is supported.
	SELECT * FROM demo WHERE pos = 1;
