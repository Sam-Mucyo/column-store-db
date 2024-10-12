#include "catalog_manager.h"

#include "common.h"
#include "utils.h"

Status init_catalog_manager(const char *db_name) {
  (void)db_name;
  cs165_log(stdout, "Initializing catalog manager for database %s\n", db_name);
  return (Status){OK, "TODO: Implement init_catalog_manager"};
}

Status create_db(const char *db_name) {
  (void)db_name;
  cs165_log(stdout, "Creating database %s\n", db_name);
  return (Status){OK, "TODO: Implement create_database"};
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
  log_info("TODO: Implement shutdown_catalog_manager\n");
  return (Status){OK, NULL};
}