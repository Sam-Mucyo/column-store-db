#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include <sys/mman.h>

#include "cs165_api.h"

#define MAX_PATH_LEN 256

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

// Initialize the catalog manager
Status init_catalog_manager(const char *db_name);

// Create a new database
Status create_db(const char *db_name);

/**
 * @brief Create a table in the given database with `num_columns` columns.
 *
 * @param db The database to create the table in.
 * @param name The name of the table to create.
 * @param num_columns The number of columns to create in the table.
 * @param status The status of the operation.
 * @return Table*
 */
Table *create_table(Db *db, const char *name, size_t num_columns, Status *status);

/**
 * @brief Create a column in the given table.
 *
 * @param table
 * @param name
 * @param sorted
 * @param ret_status
 * @return Column*
 */
Column *create_column(Table *table, char *name, bool sorted, Status *ret_status);

/**
 * @brief Get the column from catalog object
 *
 * @param db_tbl_col_name The name of the database, table, and column to get.
 *                        e.g."db1.tbl1.col1"
 * @return Column*
 */
Column *get_column_from_catalog(const char *db_tbl_col_name);

// Get a table from the catalog
Table *get_table_from_catalog(const char *table_name);

// Load data into a column
Status load_data(const char *table_name, const char *column_name, const void *data,
                 size_t num_elements);

// Shutdown the catalog manager
Status shutdown_catalog_manager();

#endif  // CATALOG_MANAGER_H