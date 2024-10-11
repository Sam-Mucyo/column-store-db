#include "cs165_api.h"

// In this class, there will always be only one active database at a time
Db *current_db;

/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table *create_table(Db *db, const char *name, size_t num_columns, Status *ret_status) {
  // void pattern for 'using' a variable to prevent compiler unused variable
  // warning
  (void)(db);
  (void)name;
  (void)num_columns;

  ret_status->code = OK;
  return NULL;
}

/*
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char *db_name) {
  // void pattern for 'using' a variable to prevent compiler unused variable
  // warning
  (void)(db_name);
  struct Status ret_status;

  ret_status.code = OK;
  return ret_status;
}

/**
 * @brief free the memory allocated for a db operator
 */
void db_operator_free(DbOperator *dbo) {
  if (dbo == NULL) {
    return;
  }

  switch (dbo->type) {
    case CREATE:
      break;

    case INSERT:
      if (dbo->operator_fields.insert_operator.values != NULL) {
        free(dbo->operator_fields.insert_operator.values);
      }
      break;

    case LOAD:
      if (dbo->operator_fields.load_operator.file_name != NULL) {
        free(dbo->operator_fields.load_operator.file_name);
      }
      break;

    default:
      break;
  }

  if (dbo->context != NULL) {
    if (dbo->context->chandle_table != NULL) {
      free(dbo->context->chandle_table);
    }
    free(dbo->context);
  }

  free(dbo);
}
