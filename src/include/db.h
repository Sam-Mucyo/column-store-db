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

typedef struct {
  char name[MAX_SIZE_NAME];
  size_t num_elements;
  DataType data_type;
  void *mmap_ptr;
  size_t mmap_size;
  long min_value;
  long max_value;
} ColumnMetadata;

typedef struct {
  char name[MAX_SIZE_NAME];
  size_t num_columns;
  ColumnMetadata *columns;
} TableMetadata;

typedef struct {
  char name[MAX_SIZE_NAME];
  size_t num_tables;
  TableMetadata *tables;
} DatabaseMetadata;

// struct ColumnIndex;

// TODO: struct alignment: consier metadata's (or most of its) data in contiguous memory
// blocks (such as column metadata) to improve cache performance
typedef struct Column {
  char name[MAX_SIZE_NAME];
  int *data;
  // You will implement column indexes later.
  void *index;
  // struct ColumnIndex *index;
  // bool clustered;
  // bool sorted;
  ColumnMetadata *metadata;
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
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
  char name[MAX_SIZE_NAME];
  Column *columns;
  size_t col_count;
  size_t table_length;
  TableMetadata *metadata;
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
  DatabaseMetadata *metadata;
} Db;

extern Db *current_db;

/*
 * Use this command to see if databases that were persisted start up properly. If
 * files don't load as expected, this can return an error.
 */
Status db_startup(void);

Status db_shutdown(void);

#endif  // DB_H
