#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "serrno.h"
#include "vmgr_api.h"

int main(argc, argv)
     int argc;
     char **argv;
{
  char buf[80];
  char *dp;
  int errflg = 0;
  FILE *fopen(), *fs;
  int media_cost;
  char *media_letter;
  char *model;
  int native_capacity;
  char *p;

  if (argc != 5) {
    fprintf (stderr,
             "usage: vmgrentermodel model media_letter native_capacity media_cost\n"
             "\n"
             "  where model            Model type         ex: SD3\n"
             "        media_letter     Media Iden         ex: B\n"
             "        native_capacity  Capacity in KBytes ex: 25000\n"
             "        media_cost       Media Cost per GB  ex: 100\n"
             "\n"
             "Comments to the CASTOR Developpment Team <castor-dev@listbox.cern.ch>\n"
             "\n");
    return(EXIT_FAILURE);
  }

  model           = argv[1];
  media_letter    = argv[2];
  native_capacity = atoi(argv[3]);
  media_cost      = atoi(argv[4]);

  if (vmgr_entermodel (model, media_letter, native_capacity, media_cost)) {
    fprintf (stderr, "%s: %s\n", model, sstrerror(serrno));
    return(EXIT_FAILURE);
  }

  printf("--> OK\n");

  return(EXIT_SUCCESS);
}
