#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "serrno.h"
#include "vmgr_api.h"

main(argc, argv)
     int argc;
     char **argv;
{
  int errflg = 0;
  char *vid;

  if (argc != 2) {
    fprintf (stderr,
             "usage: vmgrdeletetape vid\n"
             "\n"
             "  where vid              Volume ID               ex: Y16318\n"
             "\n"
             "Comments to the CASTOR Developpment Team <castor-dev@listbox.cern.ch>\n"
             "\n");
    return(EXIT_FAILURE);
  }

  vid = argv[1];

  if (vmgr_deletetape (vid)) {
    fprintf (stderr, "%s: %s\n", vid, sstrerror(serrno));
    return(EXIT_FAILURE);
  }

  printf("--> OK\n");

  return(EXIT_SUCCESS);
}
