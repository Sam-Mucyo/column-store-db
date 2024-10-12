#include "catalog_manager.h"

#include <string.h>    // For strncpy
#include <sys/stat.h>  // For mkdir
#include <sys/types.h>

#include "common.h"
#include "utils.h"

FILE *db_metadata_file = NULL;  // Declare the metadata file pointer

Status init_catalog_manager(const char *db_name) {
  (void)db_name;
  cs165_log(stdout, "Initializing catalog manager for database %s\n", db_name);
  return (Status){OK, "TODO: Implement init_catalog_manager"};
}

Status create_db(const char *db_name) {
  // cheak if the db name is valid
  if (strlen(db_name) > MAX_SIZE_NAME) {
    log_err("create_db: Database name is too long\n");
    return (Status){ERROR, "Database name is too long"};
  }
  // check if db name directory already exists
  struct stat st = {0};
  if (stat(db_name, &st) == 0) {
    log_err("create_db: Database directory already exists\n");
    return (Status){ERROR, "Database directory already exists"};
  }

  if (mkdir(db_name, 0777) == -1) {
    log_err("create_db: Failed to create %s database\n", db_name);
    return (Status){ERROR, "Failed to create %s database"};
  }

  char metadata_path[MAX_PATH_LEN];
  snprintf(metadata_path, MAX_PATH_LEN, "%s/.dbmetadata", db_name);

  // Open the metadata file and keep it open for future operations
  db_metadata_file = fopen(metadata_path, "wb+");
  if (!db_metadata_file) {
    log_err("create_db: Failed to create database metadata file\n");
    return (Status){ERROR, "Failed to create database metadata file"};
  }

  DatabaseMetadata new_db = {0};
  strncpy(new_db.name, db_name, MAX_SIZE_NAME);
  fwrite(&new_db, sizeof(DatabaseMetadata), 1, db_metadata_file);

  // NOTE: we are leaving flush to the shutdown function for performance reasons
  // This comes with a risk of data loss if the program crashes before the data is written
  // to disk or the client does not call the shutdown function (Risk to double check)
  //   fflush(db_metadata_file);  // Ensure the data is written immediately

  return (Status){OK, "Database created and metadata file kept open"};
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

void update_dbmetadata(DatabaseMetadata *updated_db) {
  if (db_metadata_file) {
    fseek(db_metadata_file, 0, SEEK_SET);  // Go to the start of the file
    size_t written = fwrite(updated_db, sizeof(DatabaseMetadata), 1, db_metadata_file);
    if (written != 1) {
      fprintf(stderr, "Error writing database metadata to file.\n");
      return;
    }

    // NOTE: same as above in create_db: leaving flush to the shutdown function for
    // performance (Risk to double check)
    // fflush(db_metadata_file); // Ensure the data is written immediately
  }
}

Status shutdown_catalog_manager(void) {
  cs165_log(stdout, "Shutting down catalog manager\n");
  if (db_metadata_file) {
    // Flush any remaining data in the buffer to the disk
    fflush(db_metadata_file);

    // Close the metadata file
    fclose(db_metadata_file);
    db_metadata_file = NULL;  // Clear the pointer to indicate it's closed
    return (Status){OK, "Catalog manager: db metadata file closed"};
  }
  log_err("L%d: shudown_catalog_manager: db metadata file was not open\n", __LINE__);
  return (Status){ERROR, "Db Metadata file was not open"};
}
