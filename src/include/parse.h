#ifndef PARSE_H__
#define PARSE_H__
#include "client_context.h"
#include "cs165_api.h"
#include "message.h"

DbOperator *parse_command(char *query_command, message *send_message, int client,
                          ClientContext *context);

#endif
