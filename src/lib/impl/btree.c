#include "btree.h"

#include <sys/types.h>

#include "btree.h"
#include "utils.h"

Btree* create_level(int* data, size_t n_elts, size_t stride, size_t fanout) {
  // n_keys = ceil(n_elts / stride)
  size_t n_keys = stride > 0 ? (n_elts + stride - 1) / stride : 0;
  if (n_keys == 0) return NULL;

  Btree* node = malloc(sizeof(Btree));
  node->keys = malloc(sizeof(int) * n_keys);
  node->n_keys = n_keys;
  node->fanout = fanout;
  node->child_ptr = NULL;

  // Fill the keys by taking every stride-th element, except the last
  size_t key_idx = 0;
  for (size_t i = 0; i < n_elts; i += stride) {
    node->keys[key_idx++] = data[i];
  }
  return node;
}

Btree* init_btree(int* data, size_t n_elts, size_t fanout) {
  if (!data || n_elts == 0 || fanout < 2) return NULL;

  // Calculate number of levels needed: this is (log_{fanout} n_elts) - 1
  //  a -1 because the last level with all values is the `data` array itself
  size_t stride = 1;
  size_t n_levels = 0;
  while (stride * fanout < n_elts) {
    stride *= fanout;
    n_levels++;
  }
  log_info("init_btree: Got %zu elements and fanout=%zu, so n_levels = %zu\n", n_elts,
           fanout, n_levels);

  // Handle single level case: we need this so that caller only cares about getting the
  //   right index when calling `lookup`
  if (n_levels == 0) {
    return create_level(data, n_elts, 1, fanout);
  }

  Btree** levels = malloc(sizeof(Btree*) * (n_levels));
  size_t current_stride = stride;

  // Create each level, starting from the root
  for (size_t i = 0; i < n_levels; i++) {
    levels[i] = create_level(data, n_elts, current_stride, fanout);
    current_stride /= fanout;
  }

  // Link the levels
  for (size_t i = 0; i < n_levels - 1; i++) {
    levels[i]->child_ptr = levels[i + 1];
  }

  // Save the root and cleanup
  Btree* root = levels[0];
  free(levels);

  return root;
}

ssize_t lookup(int key, Btree* tree) {
  if (!tree) return -1;
  //  Base cases based on our B-tree design
  if (tree->n_keys > 0) {
    if (key == tree->keys[0]) return 0;  // the min element is always the first
    if (key < tree->keys[0]) return -1;
  }

  Btree* current = tree;
  size_t pos = 0;

  cs165_log(stdout, "lookup: looking up %d in Btree\n", key);
  int level = 0;  // Just fo debugging purposes

  //   Looking up from to to bottom
  while (current && current->n_keys > 0) {
    cs165_log(stdout, "lookup: at l=%d, Node(i=%zu, values=[%d, ...]\n", level, pos,
              current->keys[pos]);

    while (pos < current->n_keys && key > current->keys[pos]) {
      cs165_log(stdout, "lookup: key=%d > curr_val=%d, moving to next\n", key,
                current->keys[pos]);
      pos++;
    }
    if (pos == current->n_keys) {
      pos--;
    }

    // Find the index of where to start reading on the next level below
    if (key < current->keys[pos]) {
      pos = current->fanout * (pos - 1);
    } else {
      pos = current->fanout * pos;
    }

    // Update for the next iteration
    current = current->child_ptr;

    level++;
    cs165_log(stdout, "moving to l=%d, pos=%zu\n", level, pos);
  }
  return pos;
}

void print_tree(Btree* tree) {
  if (!tree) return;
  printf("Node:  [");
  for (size_t i = 0; i < tree->n_keys; i++) {
    printf(" %d ", tree->keys[i]);
  }
  printf("]\n");
  if (tree->child_ptr) {
    print_tree(tree->child_ptr);
  }
}

void free_btree(Btree* tree) {
  if (!tree) return;

  if (tree->child_ptr) {
    free_btree(tree->child_ptr);
  }

  free(tree->keys);
  free(tree);
}
