#include "catalog_manager.h"

#include <dirent.h>
#include <string.h>    // For strncpy
#include <sys/stat.h>  // For mkdir
#include <sys/types.h>

#include "common.h"
#include "utils.h"

Status init_db_from_disk(void) {
  DIR *dir;
  dir = opendir(STORAGE_PATH);
  if (dir == NULL) {
    log_err("initialize_db_from_disk: Cannot open disk directory\n");
    return (Status){ERROR, "Cannot open disk directory"};
  }

  struct dirent *entry;
  char db_path[MAX_PATH_LEN];

  while ((entry = readdir(dir)) != NULL) {
    struct stat entry_stat;
    snprintf(db_path, MAX_PATH_LEN, "%s/%s", STORAGE_PATH, entry->d_name);
    if (stat(db_path, &entry_stat) == 0 && S_ISDIR(entry_stat.st_mode) &&
        strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      snprintf(db_path, MAX_PATH_LEN, "%s/%s/.dbmetadata", STORAGE_PATH, entry->d_name);

      cs165_log(stdout, "Checking for %s database's metadata\n", entry->d_name);
      FILE *metadata_file = fopen(db_path, "rb");
      if (!metadata_file) {
        log_err("L%d: in init_db_from_disk: Failed to open database metadata file\n",
                __LINE__);
        closedir(dir);
        return (Status){ERROR, "Failed to open database metadata file"};
      }
      DatabaseMetadata db_metadata;
      if (fread(&db_metadata, sizeof(DatabaseMetadata), 1, metadata_file) == 1) {
        // Initialize the database
        current_db = (Db *)malloc(sizeof(Db));
        if (!current_db) {
          fclose(metadata_file);
          closedir(dir);
          return (Status){ERROR, "Failed to allocate memory for database"};
        }

        strncpy(current_db->name, db_metadata.name, MAX_SIZE_NAME);
        current_db->tables = NULL;
        current_db->tables_size = 0;
        current_db->tables_capacity = 0;
        current_db->metadata = (DatabaseMetadata *)malloc(sizeof(DatabaseMetadata));
        if (!current_db->metadata) {
          free(current_db);
          fclose(metadata_file);
          closedir(dir);
          return (Status){ERROR, "Failed to allocate memory for database metadata"};
        }
        memcpy(current_db->metadata, &db_metadata, sizeof(DatabaseMetadata));
        fclose(metadata_file);

        // TODO: Load tables and columns from disk, based on metadata, and update
        // current_db

        closedir(dir);
        log_info("Database %s initialized from disk\n", db_metadata.name);
        return (Status){OK, "Database initialized from disk"};
      }
      fclose(metadata_file);
    }
  }
  closedir(dir);
  cs165_log(stdout, "No valid database found on disk\n");
  return (Status){ERROR, "No valid database found on disk"};
}

Status create_db(const char *db_name) {
  cs165_log(stdout, "Creating database %s\n", db_name);

  // Check if the db name is valid
  if (strlen(db_name) > MAX_SIZE_NAME) {
    log_err("create_db: Database name is too long\n");
    return (Status){ERROR, "Database name is too long"};
  }

  // Create the base "disk" directory if it doesn't exist
  struct stat st = {0};
  if (stat("disk", &st) == -1) {
    if (mkdir("disk", 0777) == -1) {
      log_err("create_db: Failed to create base 'disk' directory\n");
      return (Status){ERROR, "Failed to create base 'disk' directory"};
    }
  }

  // full path for the database directory
  char db_path[MAX_PATH_LEN];
  snprintf(db_path, MAX_PATH_LEN, "disk/%s", db_name);

  // db directory already exists?
  if (stat(db_path, &st) == 0) {
    log_err("create_db: Database directory already exists\n");
    return (Status){ERROR, "Database directory already exists"};
  }

  // Create the database directory
  if (mkdir(db_path, 0777) == -1) {
    log_err("create_db: Failed to create %s database\n", db_name);
    return (Status){ERROR, "Failed to create database"};
  }

  // Construct the metadata file path
  char metadata_path[MAX_PATH_LEN];
  snprintf(metadata_path, MAX_PATH_LEN, "%s/.dbmetadata", db_path);

  // Open the metadata file and keep it open for future operations
  FILE *db_metadata_file = fopen(metadata_path, "wb+");
  if (!db_metadata_file) {
    log_err("create_db: Failed to create database metadata file\n");
    return (Status){ERROR, "Failed to create database metadata file"};
  }

  DatabaseMetadata db_metadata = {0};
  strncpy(db_metadata.name, db_name, MAX_SIZE_NAME);
  fwrite(&db_metadata, sizeof(DatabaseMetadata), 1, db_metadata_file);
  fclose(db_metadata_file);

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
  current_db->metadata = (DatabaseMetadata *)malloc(sizeof(DatabaseMetadata));
  if (!current_db->metadata) {
    free(current_db);
    log_err("create_db: Failed to allocate memory for current_db metadata\n");
    return (Status){ERROR, "Failed to allocate memory for current_db metadata"};
  }
  memcpy(current_db->metadata, &db_metadata, sizeof(DatabaseMetadata));

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

// TODO: Revise this function to update the database metadata in memory, instead.
// void update_dbmetadata(DatabaseMetadata *updated_db) {
//   if (db_metadata_file) {
//     fseek(db_metadata_file, 0, SEEK_SET);  // Go to the start of the file
//     size_t written = fwrite(updated_db, sizeof(DatabaseMetadata), 1, db_metadata_file);
//     if (written != 1) {
//       fprintf(stderr, "Error writing database metadata to file.\n");
//       return;
//     }
//   }
// }

Status shutdown_catalog_manager(void) {
  cs165_log(stdout, "Shutting down catalog manager\n");

  if (!current_db) {
    cs165_log(stdout, "No active database to shutdown\n");
    return (Status){ERROR, "No active database to shutdown"};
  }

  // Write the current database metadata to disk
  char metadata_path[MAX_PATH_LEN];
  snprintf(metadata_path, MAX_PATH_LEN, "disk/%s/.dbmetadata", current_db->name);
  FILE *metadata_file = fopen(metadata_path, "wb");
  if (metadata_file) {
    cs165_log(stdout, "Writing database (%s) metadata to disk\n", current_db->name);
    fwrite(current_db->metadata, sizeof(DatabaseMetadata), 1, metadata_file);
    fclose(metadata_file);
  } else {
    log_err("shutdown_catalog_manager: Failed to open metadata file for writing\n");
  }

  // Free allocated memory
  for (size_t i = 0; i < current_db->tables_size; i++) {
    cs165_log(stdout, "Freeing memory for table %s\n", current_db->tables[i].name);
    free(current_db->tables[i].columns);
    // TODO: write tables metadata to disk; have a dedicated free's for table and
    // column objects
    free(current_db->tables[i].metadata);
  }
  free(current_db->tables);
  free(current_db->metadata);
  free(current_db);
  current_db = NULL;

  log_info("Catalog manager: database closed and memory freed\n");
  return (Status){OK, "Catalog manager: database closed and memory freed"};
}
