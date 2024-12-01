#include <stdio.h>

#include "algorithms.h"
#include "btree.h"

int main(void) {
  printf("\n\ntesting sort...\n");
  test_sort();

  printf("\n\ntesting btree...\n");
  test_btree();

  printf("\n\nAll tests passed!\n");

  return 0;
}
