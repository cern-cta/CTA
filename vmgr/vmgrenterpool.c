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
  char *poolname;
  uid_t pool_user;
  gid_t pool_group;

  if (argc != 4) {
    fprintf (stderr,
             "usage: vmgrenterpool poolname pool_user_uid pool_group_gid\n"
             "\n"
             "  where poolname         Pool name          ex: default\n"
             "        pool_user_uid    Pool ass. uid      ex: 0\n"
             "        pool_group_gid   Pool ass. gid      ex: 0\n"
             "\n"
             "Comments to the CASTOR Developpment Team <castor-dev@listbox.cern.ch>\n"
             "\n");
    return(EXIT_FAILURE);
  }

  poolname        = argv[1];
  pool_user       = (uid_t) atoi(argv[2]);
  pool_group      = (gid_t) atoi(argv[3]);

  if (vmgr_enterpool (poolname, pool_user, pool_group)) {
    fprintf (stderr, "%s: %s\n", poolname, sstrerror(serrno));
    return(EXIT_FAILURE);
  }

  printf("--> OK\n");

  return(EXIT_SUCCESS);
}
