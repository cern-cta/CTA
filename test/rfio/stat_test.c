#include <stdio.h>
#include <stdlib.h>
#include "rfio_api.h"

int main(argc,argv)
     int argc;
     char **argv;
{
  int i;
  struct stat statbuf;

  if (argc == 1) {
    fprintf(stderr,"Usage: %s file1_to_delete [file2_to_delete [...]]\n", argv[0]);
    exit(1);
  }

  for (i = 1; i < argc; i++) {
    if (rfio_stat(argv[i],&statbuf) != 0) {
      fprintf(stderr, "### %s error (%s)\n", argv[i], rfio_serror());
    }
  }
  exit(0);
}
