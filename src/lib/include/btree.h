#ifndef BTREE_H
#define BTREE_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

/**
 * @brief Btree node structure
 *
 * This represents an internal node in the B-tree.
 *
 * Since, we're optimizing for just lookups, we decided to:
 * - Fill factor = 100% (all internal nodes are full)
 * - internal nodes are contiguous in memory
 *      - store the keys in an array of integers
 *      - a pointer to the next level of the tree (the first element of the next level)
 * - The last level's child_ptr always points to the `data` array
 *
 * Since during lookups, we need all values to compare against, and only follow one
 * pointer (see CS165_FALL2024_Class13 slide 26 & 27).
 *
 *
 */
typedef struct Btree {
  struct Btree* child_ptr;
  int* keys;
  size_t n_keys;
  size_t fanout;
} Btree;

Btree* init_btree(int* data, size_t n_elts, size_t fanout);

/**
 * @brief Lookup a key in the B-tree.
 *
 * @param key The key to search for.
 * @param tree The root of the B-tree.
 * @return an index `i` in the `data` array such that `data[i] == key`, or -1 if not
 *         found.
 */
ssize_t lookup(int key, Btree* tree);

void print_tree(Btree* tree);

/**
 * @brief Free all memory allocated for the B-tree; not the data array.
 *
 * @param tree The root of the B-tree.
 */
void free_btree(Btree* tree);

void test_btree(void);

#endif
