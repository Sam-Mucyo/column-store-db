#ifndef SELECT_H
#define SELECT_H

#include "operators.h"

// CREATE Operations
// --------

// Executes creation of a database, table, or column
void exec_create(DbOperator *query, message *send_message);

// READ Operations
//------

// Executes a select query
void exec_select(DbOperator *query, message *send_message);
// Executes a fetch query
void exec_fetch(DbOperator *query, message *send_message);

// UPDATE Operations
//--------

// Executes an insert query
void exec_insert(DbOperator *query, message *send_message);

#endif
