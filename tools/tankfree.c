/*
 * $Id: tankfree.c,v 1.1 2005/03/30 19:56:14 jdurand Exp $
 */

/*
 * cc -I/usr/local/src/WORKDIR/PROTO2/h -o /tmp/tankfree /tmp/tankfree.c -L/usr/local/src/WORKDIR/PROTO2/shlib -lshift
 * export LD_LIBRARY_PATH=/usr/local/src/WORKDIR/PROTO2/shlib
 * /tmp/tankfree c9-1
 */

#include <stdio.h>
#include <stdlib.h>
#include "stfsperfmonlib.h"
#include "osdep.h"

char *stfsperfmon_ip = "137.138.240.58";  /* Storage Tank Performance hostname */
int   stfsperfmon_port = 5006;            /* Storage Tank Performance port */

int main(argc,argv)
     int argc;
     char **argv;
{
  unsigned long long int tank_read, tank_write, tank_total, tank_free;

  if (argc != 2) {
    fprintf(stderr,"Usage: %s container\n", argv[0]);
    exit(EXIT_FAILURE);
  }

  switch (stfsperfmon_initialize(stfsperfmon_ip, stfsperfmon_port)) {
  case STFS_PMRC_SUCCESS:
    /* Okay */
    break;
  case STFS_PMRC_BAD_ADDRESS:
    fprintf(stderr,"stfsperfmon_initialize error: Bad Address IP: %s, Port: %d\n", stfsperfmon_ip, stfsperfmon_port);
    exit(EXIT_FAILURE);
  case STFS_PMRC_NOT_CONNECTED:
    fprintf(stderr,"stfsperfmon_initialize error: Initial connection to IP: %s Port %d failed.\n", stfsperfmon_ip, stfsperfmon_port);
    exit(EXIT_FAILURE);
  default:
    fprintf(stderr,"stfsperfmon_initialize error: Unknown failure with IP: %s Port %d\n", stfsperfmon_ip, stfsperfmon_port);
    exit(EXIT_FAILURE);
  }

  switch (stfsperfmon_get_fs_statistics(argv[1], &tank_read, &tank_write, &tank_total, &tank_free)) {
  case 0:
    /* Okay */
    break;
  default:
    fprintf(stderr,"stfsperfmon_get_fs_statistics error\n");
    exit(EXIT_FAILURE);
  }

  fprintf(stdout,"Free/Total space on %s: %dG/%dG\n", argv[1], (int) (tank_free / ONE_GB), (int) (tank_total / ONE_GB));

  exit(EXIT_SUCCESS);

}
