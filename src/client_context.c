#include "client_context.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

ClientContext *g_client_context = NULL;

// Initialize the global ClientContext
void init_client_context(void) {
  if (g_client_context == NULL) {
    g_client_context = (ClientContext *)malloc(sizeof(ClientContext));
    g_client_context->chandle_table = NULL;
    g_client_context->chandles_in_use = 0;
    g_client_context->chandle_slots = 0;
    g_client_context->variables_in_use = 0;
  }
}

// Get the global ClientContext
ClientContext *get_client_context(void) { return g_client_context; }

// Free the global ClientContext
void free_client_context(void) {
  log_info("Freeing client context\n");
  cs165_log(stdout, "a preview of handles and their names\n");
  for (int i = 0; i < g_client_context->chandles_in_use; i++) {
    cs165_log(stdout, "chandle %d: %s\n", i, g_client_context->chandle_table[i].name);
  }
  if (g_client_context != NULL) {
    free(g_client_context->chandle_table);
    free(g_client_context);
    g_client_context = NULL;
  }
}

// Add a new handle to the chandle_table
Status add_handle(const char *name, GeneralizedColumn *gen_col) {
  Status status = {OK, NULL};

  if (g_client_context->chandles_in_use >= g_client_context->chandle_slots) {
    // Resize chandle_table if necessary
    int new_size =
        g_client_context->chandle_slots == 0 ? 1 : g_client_context->chandle_slots * 2;
    g_client_context->chandle_table = realloc(g_client_context->chandle_table,
                                              new_size * sizeof(GeneralizedColumnHandle));
    if (g_client_context->chandle_table == NULL) {
      status.code = ERROR;
      status.error_message = "Failed to allocate memory for chandle_table";
      return status;
    }
    g_client_context->chandle_slots = new_size;
  }

  GeneralizedColumnHandle *chandle =
      &g_client_context->chandle_table[g_client_context->chandles_in_use];
  strncpy(chandle->name, name, HANDLE_MAX_SIZE);
  chandle->generalized_column = *gen_col;
  g_client_context->chandles_in_use++;

  return status;
}

// Get a handle from the chandle_table
GeneralizedColumn *get_handle(const char *name) {
  // use most recent handle
  for (int i = g_client_context->chandles_in_use - 1; i >= 0; i--) {
    if (strcmp(g_client_context->chandle_table[i].name, name) == 0) {
      return &g_client_context->chandle_table[i].generalized_column;
    }
  }
  return NULL;
}
