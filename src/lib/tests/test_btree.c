#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "btree.h"
#include "test_helpers.h"
#include "utils.h"

// Main test function
void test_btree_lookup(int* data, size_t data_size, int fanout) {
  if (data == NULL || data_size == 0 || fanout < 2) {
    printf("Invalid input parameters\n");
    return;
  }

  printf("Data size: %zu, Fanout: %d\n", data_size, fanout);

  // Initialize B-tree
  Btree* tree = init_btree(data, data_size, fanout);
  printf("\nB-tree structure:\n");
  print_tree(tree);

  // Generate test cases based on the fanout
  size_t stride = fanout;

  // Test exact matches (elements at fanout boundaries)
  test_sub_title("Testing exact matches at fanout boundaries");
  for (size_t i = 0; i < data_size; i += stride) {
    int key = data[i];
    size_t pos = lookup(key, tree);
    assert_nice(pos, i, ", ");
  }

  // Test shifted matches (elements between fanout boundaries)
  test_sub_title("Testing shifted matches between fanout boundaries");
  for (size_t i = stride / 2; i < data_size; i += stride) {
    if (i >= data_size) break;
    int key = data[i];
    size_t pos = lookup(key, tree);
    // Expected position is the previous fanout boundary
    size_t expected_pos = (i / stride) * stride;
    assert_nice(pos, expected_pos, ", ");
  }

  // Test boundary conditions
  test_sub_title("Testing boundary conditions");

  // Test minimum value
  ssize_t pos = lookup(data[0] - 1, tree);
  assert_nice(pos, -1, ", ");

  // Test maximum value
  pos = lookup(data[data_size - 1] + 1, tree);
  assert_nice(pos, ((data_size - 1) / stride) * stride, "\n");

  // Cleanup
  free_btree(tree);
  printf("\nAll tests completed successfully!\n");
}

void test_btree(void) {
  {
    test_title("Test 1: Just print for a nice tree\n");
    int sorted_data[] = {10, 20, 30, 40, 50, 60, 70, 80};
    int n = sizeof(sorted_data) / sizeof(int);

    Btree* root = init_btree(sorted_data, n, 2);
    print_tree(root);

    printf("data: [");
    for (int i = 0; i < n; i++) {
      printf(" %d ", sorted_data[i]);
    }
    printf("]\n");
    free_btree(root);
  }

  {
    test_title("\nTest 2: Just print a tree: n_elts and fanout don't work nicely\n");
    int sorted_data[] = {10, 20, 30, 40, 50, 60, 70, 80};
    int n = sizeof(sorted_data) / sizeof(int);

    Btree* root = init_btree(sorted_data, n, 3);
    print_tree(root);

    printf("data: [");
    for (int i = 0; i < n; i++) {
      printf(" %d ", sorted_data[i]);
    }
    printf("]\n");

    free_btree(root);
  }

  {
    /*

            B-tree structure:
                    [child_ptr][10,50]
                            |
                [child_ptr][10,30,50,70]
                     |
        data        [10 20 30 40 50 60 70 80]
        index       [ 0  1  2  3  4  5  6  7]
     */
    test_title("\nTest 3: Lookup in a B-tree with fanout 2 and aligned data\n");
    int data[] = {10, 20, 30, 40, 50, 60, 70, 80};
    size_t data_size = sizeof(data) / sizeof(data[0]);
    int fanout = 2;

    test_btree_lookup(data, data_size, fanout);
  }
  {
    test_title("\nTest 4: Lookup in a B-tree with fanout 5 and Not aligned data\n");
    int data[120];
    for (int i = 0; i < 120; i++) {
      data[i] = i * 10;
    }
    size_t data_size = sizeof(data) / sizeof(data[0]);
    int fanout = 5;

    test_btree_lookup(data, data_size, fanout);
  }
}