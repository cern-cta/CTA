#include <stdio.h>
#include <stdlib.h>
#include "rfio_api.h"

int main(argc,argv)
     int argc;
     char **argv;
{
  int i;

  if ((argc == 1) || ((argc - 1) % 2 != 0)) {
    fprintf(stderr,"Usage: %s file1 symlinkfile1 [file2 symlinkfile2 [...]]\n", argv[0]);
    exit(1);
  }

  for (i = 1; i < argc; i += 2) {
    if (rfio_symlink(argv[i], argv[i+1]) != 0) {
      fprintf(stderr, "### %s -> %s error (%s)\n", argv[i+1], argv[i], rfio_serror());
    }
  }
  exit(0);
}
