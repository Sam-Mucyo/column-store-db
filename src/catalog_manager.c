#include "catalog_manager.h"

#include <dirent.h>
#include <string.h>    // For strncpy
#include <sys/stat.h>  // For mkdir
#include <sys/types.h>

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
  return (Status){OK, "Database created and current_db initialized"};
}

Table *create_table(Db *db, const char *name, size_t num_columns, Status *ret_status) {
  (void)db;
  (void)name;
  (void)num_columns;
  (void)ret_status;
  cs165_log(stdout, "Creating table %s with %zu columns\n", name, num_columns);
  log_info("TODO: Implement create_table\n");
  return NULL;
}

Column *create_column(Table *table, char *name, bool sorted, Status *ret_status) {
  (void)table;
  (void)name;
  (void)sorted;
  (void)ret_status;
  cs165_log(stdout, "Creating column %s in table %s\n", name, table->name);
  log_info("TODO: Implement create_column\n");
  return NULL;
}

Column *get_column_from_catalog(const char *db_tbl_col_name) {
  (void)db_tbl_col_name;
  cs165_log(stdout, "catalog_manager: Get column with args %s\n", db_tbl_col_name);
  log_info("TODO: Implement get_column_from_catalog\n");
  return NULL;
}

Table *get_table_from_catalog(const char *table_name) {
  (void)table_name;
  cs165_log(stdout, "catalog_manager: Get table %s\n", table_name);
  log_info("TODO: Implement get_table_from_catalog\n");
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
  log_err("shutdown_catalog_manager: Not implemented\n");
  return (Status){OK, "Catalog manager: database closed and memory freed"};
}
