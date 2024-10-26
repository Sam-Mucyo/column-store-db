#define _GNU_SOURCE
#include "utils.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#define LOG 1
#define LOG_ERR 1
#define LOG_INFO 1

/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */
char *trim_newline(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!(str[i] == '\r' || str[i] == '\n')) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */
char *trim_whitespace(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!isspace(str[i])) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters. */
char *trim_parenthesis(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!(str[i] == '(' || str[i] == ')')) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}

char *trim_quotes(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (str[i] != '\"') {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}
/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
void cs165_log(FILE *out, const char *format, ...) {
#ifdef LOG
  va_list v;
  va_start(v, format);
  vfprintf(out, format, v);
  va_end(v);
#else
  (void)out;
  (void)format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
  va_list v;
  va_start(v, format);
  fprintf(stderr, ANSI_COLOR_RED);
  vfprintf(stderr, format, v);
  fprintf(stderr, ANSI_COLOR_RESET);
  va_end(v);
#else
  (void)format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
  va_list v;
  va_start(v, format);
  fprintf(stdout, ANSI_COLOR_GREEN);
  vfprintf(stdout, format, v);
  fprintf(stdout, ANSI_COLOR_RESET);
  fflush(stdout);
  va_end(v);
#else
  (void)format;
#endif
}

void handle_error(message *send_message, char *error_message) {
  send_message->status = EXECUTION_ERROR;
  send_message->payload = error_message;
  send_message->length = strlen(send_message->payload);
}

/**
 * @brief Extends memory-mapped region if necessary and updates values
 *
 * This function will extend the mapped memory region if needed to accommodate
 * new values, but does not immediately sync changes to disk. The actual file
 * extension and disk synchronization should be handled separately.
 *
 * @param mapped_addr Pointer to the current memory-mapped region
 * @param current_size Pointer to current size of mapped region in bytes
 * @param offset Starting position (in number of integers) where new values will be
 * written
 * @param new_values Array of integer values to write
 * @param count Number of integers to write
 *
 * @return Pointer to (possibly new) mapped region on success, NULL on failure
 *         If successful, *current_size will be updated if extension occurred
 *
 * @note If extension occurs, the old mapped_addr may become invalid and the
 *       new returned pointer should be used instead
 */
int *extend_and_update_mmap(int *mapped_addr, size_t *current_size, size_t offset,
                            const int *new_values, size_t count) {
  if (mapped_addr == NULL || current_size == NULL || new_values == NULL || count == 0) {
    errno = EINVAL;
    return NULL;
  }

  // Calculate required size
  size_t required_size = (offset + count) * sizeof(int);

  // Check if we need to extend
  if (required_size > *current_size) {
    // Round up to nearest page size for efficiency
    size_t page_size = sysconf(_SC_PAGESIZE);
    cs165_log(stdout, "extend_and_update_mmap: page size %zu\n", page_size);
    size_t new_size = (required_size + page_size - 1) & ~(page_size - 1);

    // Remap with new size
    void *new_addr = mremap(mapped_addr, *current_size, new_size, MREMAP_MAYMOVE);
    if (new_addr == MAP_FAILED) {
      return NULL;
    }

    mapped_addr = new_addr;
    *current_size = new_size;
  }

  // Copy new values to the specified offset
  memcpy(mapped_addr + offset, new_values, count * sizeof(int));

  return mapped_addr;
}
