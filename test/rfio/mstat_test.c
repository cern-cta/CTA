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
    fprintf(stderr,"Usage: %s file1_to_stat [file2_to_stat [...]]\n", argv[0]);
    exit(1);
  }

  for (i = 1; i < argc; i++) {
    if (rfio_mstat(argv[i],&statbuf) != 0) {
      fprintf(stderr, "### %s error (%s)\n", argv[i], rfio_serror());
    }
  }
  rfio_end();
  exit(0);
}
