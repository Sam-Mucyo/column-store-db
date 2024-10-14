#include "catalog_manager.h"

#include <dirent.h>
#include <string.h>    // For strncpy
#include <sys/stat.h>  // For mkdir
#include <sys/types.h>
#include <unistd.h>

#include "common.h"
#include "utils.h"

Status init_db_from_disk(void) {
  log_err("init_db_from_disk: Not implemented\n");
  return (Status){ERROR, "No valid database found on disk"};
}

int create_directory(const char *path) {
  struct stat st = {0};
  if (stat(path, &st) == -1) {
    if (mkdir(path, 0777) == -1) {
      log_err("create_directory: Failed to create %s directory\n", path);
      return -1;
    }
  }
  return 0;
}

Status create_db(const char *db_name) {
  cs165_log(stdout, "Creating database %s\n", db_name);

  // Check if the db name is valid
  if (strlen(db_name) > MAX_SIZE_NAME) {
    log_err("create_db: Database name is too long\n");
    return (Status){ERROR, "Database name is too long"};
  }

  // Create the base "disk" directory if it doesn't exist
  if (create_directory(STORAGE_PATH) == -1) {
    return (Status){ERROR, "Failed to create disk directory"};
  }

  // full path for the database directory
  char db_path[MAX_PATH_LEN];
  snprintf(db_path, MAX_PATH_LEN, "disk/%s", db_name);

  // db directory
  if (create_directory(db_path) == -1) {
    return (Status){ERROR, "Failed to create database directory"};
  }

  // Set up the global current_db object
  current_db = (Db *)malloc(sizeof(Db));
  if (!current_db) {
    log_err("create_db: Failed to allocate memory for current_db\n");
    return (Status){ERROR, "Failed to allocate memory for current_db"};
  }

  strncpy(current_db->name, db_name, MAX_SIZE_NAME);
  current_db->tables = NULL;
  current_db->tables_size = 0;
  current_db->tables_capacity = 0;
  log_info("Database %s created successfully\n", db_name);
  return (Status){OK, "Database created and current_db initialized"};
}

Table *create_table(Db *db, const char *name, size_t num_columns, Status *status) {
  cs165_log(stdout, "Creating table %s in database %s\n", name, db->name);

  // NOTE: Maybe we should check if the table already exists in the database
  if (strlen(name) >= MAX_SIZE_NAME || num_columns <= 0) {
    *status = (Status){ERROR, "Invalid table name or number of columns"};
    return NULL;
  }

  // Create the table directory
  char table_path[MAX_PATH_LEN];
  snprintf(table_path, MAX_PATH_LEN, "%s/%s/%s", STORAGE_PATH, db->name, name);
  if (create_directory(table_path) != 0) {
    log_err("create_table: Failed to create table directory\n");
    *status = (Status){ERROR, "Failed to create table directory"};
    return NULL;
  }

  // Check if we need to resize the tables array
  if (db->tables_size >= db->tables_capacity) {
    cs165_log(stdout, "Resizing tables array curr capacity: %zu\n", db->tables_capacity);
    size_t new_capacity = db->tables_capacity == 0 ? 1 : db->tables_capacity * 2;
    Table *new_tables = realloc(db->tables, new_capacity * sizeof(Table));
    if (!new_tables) {
      *status = (Status){ERROR, "Failed to resize tables array"};
      return NULL;
    }
    db->tables = new_tables;
    db->tables_capacity = new_capacity;
  }

  // Initialize the new table
  Table *new_table = &db->tables[db->tables_size];
  strncpy(new_table->name, name, MAX_SIZE_NAME);
  new_table->columns = calloc(num_columns, sizeof(Column));
  if (!new_table->columns) {
    log_err("create_table: Failed to allocate memory for requested %zu columns\n",
            num_columns);
    *status = (Status){ERROR, "Memory allocation failed for columns"};
    return NULL;
  }

  new_table->col_capacity = num_columns;
  new_table->num_cols = 0;

  db->tables_size++;
  log_info("Table %s created successfully\n", name);
  *status = (Status){OK, "Table created successfully"};
  return new_table;
}

Column *create_column(Table *table, char *name, bool sorted, Status *ret_status) {
  (void)sorted;
  cs165_log(stdout, "Creating column %s in table %s\n", name, table->name);

  if (strlen(name) >= MAX_SIZE_NAME) {
    log_err("create_column: Column name is too long\n");
    *ret_status = (Status){ERROR, "Column name is too long"};
    return NULL;
  }

  // Check if we need to resize the columns array
  if (table->num_cols >= table->col_capacity) {
    log_err("create_column: Table %s is full\n", table->name);
    *ret_status = (Status){ERROR, "Table is full"};
    return NULL;
  }

  // Initialize the new column
  Column *new_column = &table->columns[table->num_cols];
  strncpy(new_column->name, name, MAX_SIZE_NAME);
  new_column->data = NULL;
  new_column->num_elements = 0;
  new_column->min_value = 0;
  new_column->max_value = 0;
  new_column->mmap_size = 0;

  table->num_cols++;
  log_info("Column %s created successfully\n", name);
  *ret_status = (Status){OK, "Column created successfully"};
  return new_column;
}

/**
 * @brief Get the column from catalog object
 *
 * @param db_tbl_col_name e.g."db1.tbl1.col1"
 * @return Column*
 */
Column *get_column_from_catalog(const char *db_tbl_col_name) {
  cs165_log(stdout, "catalog_manager: Get column %s from catalog\n", db_tbl_col_name);
  // Ensure the current database is set
  if (current_db == NULL) {
    log_err("get_column_from_catalog: No database loaded");
    return NULL;
  }

  // Split the db_tbl_col_name into its components
  char db_name[MAX_SIZE_NAME];
  char table_name[MAX_SIZE_NAME];
  char column_name[MAX_SIZE_NAME];
  if (sscanf(db_tbl_col_name, "%[^.].%[^.].%s", db_name, table_name, column_name) != 3) {
    log_err("get_column_from_catalog: Invalid db_tbl_col_name format");
    return NULL;
  }

  // Search for the column within the current database
  for (size_t i = 0; i < current_db->tables_size; i++) {
    if (strcmp(current_db->tables[i].name, table_name) == 0) {
      for (size_t j = 0; j < current_db->tables[i].num_cols; j++) {
        if (strcmp(current_db->tables[i].columns[j].name, column_name) == 0) {
          log_info("get_column_from_catalog: Column %s found\n", column_name);
          return &current_db->tables[i].columns[j];
        }
      }
    }
  }
  return NULL;
}

Table *get_table_from_catalog(const char *table_name) {
  // Ensure the current database is set
  if (current_db == NULL) {
    log_err("get_table_from_catalog: No database loaded");
    return NULL;
  }

  // Search for the table within the current database
  for (size_t i = 0; i < current_db->tables_size; i++) {
    if (strcmp(current_db->tables[i].name, table_name) == 0) {
      return &current_db->tables[i];
    }
  }

  log_err("get_table_from_catalog: Table not found");
  return NULL;
}

Status load_data(const char *table_name, const char *column_name, const void *data,
                 size_t num_elements) {
  (void)table_name;
  (void)column_name;
  (void)data;
  (void)num_elements;
  cs165_log(stdout, "catalog_manager: Load data into column %s in table %s\n",
            column_name, table_name);
  log_info("TODO: Implement load_data\n");
  return (Status){OK, NULL};
}

Status shutdown_catalog_manager(void) {
  cs165_log(stdout, "Shutting down catalog manager\n");
  if (!current_db) {
    cs165_log(stdout, "No active database to shutdown\n");
    return (Status){ERROR, "No active database to shutdown"};
  }

  // Free the tables and columns
  for (size_t i = 0; i < current_db->tables_size; i++) {
    // free mmap'd memory column data
    for (size_t j = 0; j < current_db->tables[i].num_cols; j++) {
      if (current_db->tables[i].columns[j].data) {
        cs165_log(stdout, "Freeing memory for column %s\n",
                  current_db->tables[i].columns[j].name);
        cs165_log(stdout, "data at i=0: %d\n", current_db->tables[i].columns[j].data[0]);
        int last = current_db->tables[i].columns[j].num_elements - 1;
        cs165_log(stdout, "data at last: %d\n",
                  current_db->tables[i].columns[j].data[last]);
        if (munmap(current_db->tables[i].columns[j].data,
                   current_db->tables[i].columns[j].mmap_size) == -1) {
          perror("Error unmapping memory");
        }
        // close if the file descriptor is valid
        if (current_db->tables[i].columns[j].disk_fd > 0) {
          cs165_log(stdout, "closing fd: %d\n", current_db->tables[i].columns[j].disk_fd);
          close(current_db->tables[i].columns[j].disk_fd);
        } else {
          log_err("couldn't find fd for column %s\n",
                  current_db->tables[i].columns[j].name);
        }
      }
    }
    free(current_db->tables[i].columns);
  }
  free(current_db->tables);
  return (Status){OK, "Catalog manager: database closed and memory freed"};
}
