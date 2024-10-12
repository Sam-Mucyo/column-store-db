#include "cs165_api.h"

// In this class, there will always be only one active database at a time
Db *current_db;

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
