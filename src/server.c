/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/** server.c
 * CS165 Fall 2024
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "catalog_manager.h"
#include "client_context.h"
#include "common.h"
#include "parse.h"
#include "query_handler.h"
#include "utils.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

int handle_csv_transfer(int client_socket);

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket, int *shutdown) {
  int done = 0;
  int length = 0;

  log_info("Connected to socket: %d.\n", client_socket);

  // Create two messages, one from which to read and one from which to receive
  message send_message;
  message recv_message;

  // create the client context here
  ClientContext *client_context = NULL;

  // Continually receive messages from client and execute queries.
  // 1. Parse the command
  // 2. Handle request if appropriate
  // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
  // 4. Send response to the request.

  int csv_transfer_end = 0;

  do {
    length = recv(client_socket, &recv_message, sizeof(message), 0);
    if (length < 0) {
      log_err("Client connection closed!\n");
    } else if (length == 0) {
      done = 1;
    }

    printf("Received message with status: %d\n", recv_message.status);
    if (!done) {
      // TODO: transfer_end is a hack to handle the CSV transfer. Ask in OH: why, without
      // this, the server keeps receiving msg.status = CSV_TRANSFER_START regardless of
      // the client's message.
      if (recv_message.status == CSV_TRANSFER_START && !csv_transfer_end) {
        // Handle CSV transfer
        cs165_log(stdout, "Server: Receiving CSV transfer\n");
        if (handle_csv_transfer(client_socket) < 0) {
          log_err("L%d: Failed to handle CSV transfer.\n", __LINE__);
        } else {
          log_info("Server: CSV transfer successful\n");
        }
        recv_message.status = CSV_TRANSFER_END;
        csv_transfer_end = 1;
      } else {
        char recv_buffer[recv_message.length + 1];
        length = recv(client_socket, recv_buffer, recv_message.length, 0);
        recv_message.payload = recv_buffer;
        recv_message.payload[recv_message.length] = '\0';

        // 1. Parse command
        //    Query string is converted into a request for an database operator
        DbOperator *query = parse_command(recv_message.payload, &send_message,
                                          client_socket, client_context);

        if (query != NULL && query->type == SHUTDOWN) {
          *shutdown = 1;
          done = 1;
        }
        // 2. Handle request
        //    Corresponding database operator is executed over the query
        // TODO: Make the `execute_DbOperator` return a `message` struct, the status
        // depends on the query
        char *result = execute_DbOperator(query);

        send_message.length = strlen(result);
        char send_buffer[send_message.length + 1];
        strcpy(send_buffer, result);
        send_message.payload = send_buffer;

        // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
        if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
          log_err("Failed to send message with error: %s\n", strerror(errno));
        }

        // 4. Send response to the request
        if (send(client_socket, result, send_message.length, 0) == -1) {
          log_err("Failed to send message.");
        }
      }
    }
  } while (!done);

  log_info("Connection closed at socket %d!\n", client_socket);
  close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server(void) {
  int server_socket;
  size_t len;
  struct sockaddr_un local;

  log_info("Attempting to setup server...\n");

  if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    log_err("L%d: Failed to create socket.\n", __LINE__);
    return -1;
  }

  local.sun_family = AF_UNIX;
  strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  unlink(local.sun_path);

  /*
  int on = 1;
  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on,
  sizeof(on)) < 0)
  {
      log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
      return -1;
  }
  */

  len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
  if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
    log_err("L%d: Socket failed to bind.\n", __LINE__);
    return -1;
  }

  if (listen(server_socket, 5) == -1) {
    log_err("L%d: Failed to listen on socket.\n", __LINE__);
    return -1;
  }

  // after all setup, setup db. TODO: check implications of this on concurrent clients
  db_startup();

  return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
//
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients?
//      Is there a maximum number of concurrent client connections you will
//      allow? What aspects of siloes or isolation are maintained in your
//      design? (Think `what` is shared between `whom`?)
int main(void) {
  int server_socket = setup_server();
  if (server_socket < 0) {
    exit(1);
  }

  log_info("Waiting for a connection %d ...\n", server_socket);

  struct sockaddr_un remote;
  socklen_t t = sizeof(remote);
  int client_socket = 0;
  int shutdown = 0;
  while (!shutdown) {
    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
      log_err("L%d: Failed to accept a new connection.\n", __LINE__);
    }
    handle_client(client_socket, &shutdown);
  }
  return 0;
}

int handle_csv_transfer(int client_socket) {
  CSVChunk chunk;
  int fd = -1;
  Column *col = NULL;
  size_t total_received = 0;
  size_t column_received = 0;
  size_t column_total_size = 0;

  while (1) {
    ssize_t bytes_received = recv(client_socket, &chunk, sizeof(CSVChunk), 0);
    if (bytes_received <= 0) {
      break;
    }

    if (strcmp(chunk.column_name, "END_TRANSMISSION") == 0) {
      break;
    }

    if (fd == -1) {
      char filename[512];
      // NOTE: move this file in its respective directory based on db.table.column
      // will need to clean up since this a similar functionality as in parse.c
      snprintf(filename, sizeof(filename), "%s.bin", chunk.column_name);
      fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
      if (fd == -1) {
        log_err("Error opening file");
        close(fd);
        return -1;
      }
      //   Ensure the column exists in the catalog
      col = get_column_from_catalog(chunk.column_name);
      if (!col) {
        log_err("Column not found in catalog");
        close(fd);
        return -1;
      }
      column_total_size = chunk.total_size;

      // Extend file to the size specified by the client
      if (ftruncate(fd, column_total_size) == -1) {
        log_err("Error extending file");
        close(fd);
        return -1;
      }
      col->num_elements = column_total_size / sizeof(int);
      col->mmap_size = column_total_size;  // NOTE: cleanup later after making this
                                           // generic with DATA_TYPE
      col->disk_fd = fd;
      col->data =
          mmap(NULL, column_total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if (col->data == MAP_FAILED) {
        log_err("Error mmapping file");
        close(fd);
        return -1;
      }

      column_received = 0;
      cs165_log(stdout, "Started receiving column: %s (Expected size: %zu bytes)\n",
                chunk.column_name, column_total_size);
    }

    memcpy(col->data + column_received, chunk.data, chunk.chunk_size);
    column_received += chunk.chunk_size;
    total_received += chunk.chunk_size;

    cs165_log(stdout, "Received %d bytes for column %s (Total: %zu bytes)\n",
              chunk.chunk_size, chunk.column_name, column_received);

    if (column_received >= column_total_size) {
      fd = -1;
      // Leave mmap'd memory open for other queries: select, fetch, etc.
      // Shutdown catalog manager, will handle this.
      cs165_log(stdout, "Finished receiving column: %s (Total: %zu bytes)\n",
                chunk.column_name, column_received);
    }
  }

  // Leave mmap'd memory open for other queries: select, fetch, etc.
  // Shutdown catalog manager, will handle this.
  //   if (map) {
  //     munmap(map, column_total_size);
  //   }
  //   if (fd != -1) {
  //     close(fd);
  //   }
  cs165_log(stdout, "Server finished processing data. Total received: %zu bytes\n",
            total_received);

  // Send confirmation message to client
  message send_message;
  send_message.length = strlen("-- transfer successful\n");
  send_message.status = OK_DONE;

  if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
    log_err("Failed to send confirmation message header");
    return -1;
  }

  if (send(client_socket, "-- transfer successful\n", send_message.length, 0) == -1) {
    log_err("Failed to send confirmation message");
    return -1;
  }
  return 0;
}
