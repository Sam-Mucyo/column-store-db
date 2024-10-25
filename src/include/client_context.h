#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "db.h"

/*
 * holds the information necessary to refer to a result column
 */
typedef struct ClientContext {
  Column *chandle_table;
  int chandles_in_use;
  int chandle_slots;
} ClientContext;

extern ClientContext *g_client_context;

void init_client_context();
void free_client_context();
int create_new_handle(const char *name, Column **out_column);
Column *get_handle(const char *name);

#endif
