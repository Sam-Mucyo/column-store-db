#ifndef DB_H
#define DB_H

#include <stdlib.h>

#include "common.h"

/*
 * Declares the type of a result column,
 which includes the number of tuples in the result, the data type of the result, and
 a pointer to the result data
 */
typedef struct Result {
  size_t num_tuples;
  DataType data_type;
  void *payload;
} Result;

typedef struct Column {
  char name[MAX_SIZE_NAME];
  int *data;
  //   void *index;
  size_t num_elements;
  long min_value;
  long max_value;
  size_t mmap_size;
} Column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_capacity, the maximum number of columns that can be held in the table.
 * - columns this is the pointer to an array of columns contained in the table.
 * - num_cols, the number of columns currently held in the table.
 **/

typedef struct Table {
  char name[MAX_SIZE_NAME];
  Column *columns;
  size_t col_capacity;
  size_t num_cols;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently
 *allocated memory slot
 **/

typedef struct Db {
  char name[MAX_SIZE_NAME];
  Table *tables;
  size_t tables_size;
  size_t tables_capacity;
} Db;

extern Db *current_db;

/*
 * Use this command to see if databases that were persisted start up properly. If
 * files don't load as expected, this can return an error.
 */
Status db_startup(void);

Status db_shutdown(void);

#endif  // DB_H
