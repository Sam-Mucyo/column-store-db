#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "db.h"

#define MAX_VARIABLES 100

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType { RESULT, COLUMN } GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
  Result *result;
  Column *column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
  GeneralizedColumnType column_type;
  GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
  char name[HANDLE_MAX_SIZE];
  GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or
 * columns)
 */
typedef struct ClientContext {
  GeneralizedColumnHandle *chandle_table;
  int chandles_in_use;
  int chandle_slots;
  GeneralizedColumnHandle variable_pool[MAX_VARIABLES];
  int variables_in_use;
} ClientContext;

extern ClientContext *g_client_context;

void init_client_context();
ClientContext *get_client_context();
void free_client_context();
Status add_handle(const char *name, GeneralizedColumn *gen_col);
GeneralizedColumn *get_handle(const char *name);

#endif
