#include <stdio.h>
#include <stdlib.h>
#include "u64subr.h"
#include "Cns_api.h"
#include "serrno.h"

int main(int argc, char**argv) {

  char checkpath[CA_MAXPATHLEN+1];

  if (argc != 3) {
    fprintf(stderr,"Usage: %s server fileid\n", argv[0]);
    exit(1);
  }

  // Call name server
  if (Cns_getpath(argv[1], strtou64(argv[2]), checkpath) != 0) {
    fprintf(stderr, "Server %s Fileid %s, %s\n",
            argv[1], argv[2], sstrerror(serrno));
    exit(1);
  }

  // display result
  fprintf(stdout, "%s\n", checkpath);
  exit(0);
}
