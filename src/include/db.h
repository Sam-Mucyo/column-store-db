#ifndef DB_H
#define DB_H

#include <stdlib.h>

#include "common.h"
typedef struct Column {
  char name[MAX_SIZE_NAME];
  DataType data_type;
  void *data;
  size_t mmap_size;  // can be derived from mmap_size (clean up later), also a result
                     // column doesn't need to have this; what'd this mean for `insert`?
  int disk_fd;
  //   void *index;
  size_t num_elements;
  // Stat metrics
  long min_value;
  long max_value;
  long sum;
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

void db_shutdown(void);

#endif  // DB_H
