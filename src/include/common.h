// This file includes shared constants and other values.
#ifndef COMMON_H__
#define COMMON_H__

// define the socket path if not defined.
// note on windows we want this to be written to a docker container-only path
#ifndef SOCK_PATH
#define SOCK_PATH "cs165_unix_socket"
#endif

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define MAX_PATH_LEN 256

/**
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/
typedef enum DataType { INT, LONG, FLOAT } DataType;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
  StatusCode code;
  char *error_message;
} Status;

// mesage_status defines the status of the previous request.
// FEEL FREE TO ADD YOUR OWN OR REMOVE ANY THAT ARE UNUSED IN YOUR PROJECT
typedef enum message_status {
  OK_DONE,
  OK_WAIT_FOR_RESPONSE,
  SERVER_SHUTDOWN,
  UNKNOWN_COMMAND,
  QUERY_UNSUPPORTED,
  OBJECT_ALREADY_EXISTS,
  OBJECT_NOT_FOUND,
  INCORRECT_FORMAT,
  EXECUTION_ERROR,
  INCORRECT_FILE_FORMAT,
  FILE_NOT_FOUND,
  INDEX_ALREADY_EXISTS
} message_status;

// message is a single packet of information sent between client/server.
// message_status: defines the status of the message.
// length: defines the length of the string message to be sent.
// payload: defines the payload of the message.
typedef struct message {
  message_status status;
  int length;
  char *payload;
} message;

#endif  // COMMON_H__
