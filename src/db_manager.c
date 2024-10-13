#include "catalog_manager.h"
#include "operators.h"
#include "utils.h"

// In this class, there will always be only one active database at a time
Db *current_db;

Status db_startup(void) {
  cs165_log(stdout, "Startup server\n");
  init_db_from_disk();
  return (Status){OK, NULL};
}

Status db_shutdown(void) {
  shutdown_catalog_manager();
  cs165_log(stdout, "Shutdown server\n");
  return (Status){OK, NULL};
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
