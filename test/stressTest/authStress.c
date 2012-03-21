// A simple nsping with a loop over the API call to do performance testing
// on secure authentication

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "getconfent.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]\n", name);
    printf ("Perform nsping as fast as possible and report results.\n\n");
    printf ("  -h, --host=HOSTNAME  the name server to ping\n");
    printf ("      --help           display this help and exit\n\n");
    printf ("Report bugs to <andrea.ieri@cern.ch>.\n");
  }
  exit (status);
}

int ping (char *server){
  char info[256];
  Cns_ping (server, info);
  return 0;
}

int main(int argc,
         char **argv)
{
  int c;
  int errflg = 0;
  int hflg = 0;
  int repetitions = 2000;
  char info[256];
  static char retryenv[16];
  char *server = "localhost";

  Coptions_t longopts[] = {
    { "host", REQUIRED_ARGUMENT, NULL, 'h' },
    { "help", NO_ARGUMENT,       &hflg, 1  },
    { NULL,   0,                 NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "h:", longopts, NULL)) != EOF) {
    switch (c) {
    case 'h':
      server = Coptarg;
      setenv(CNS_HOST_ENV, server, 1);
      break;
    case '?':
    case ':':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (hflg) {
    usage (0, argv[0]);
  }
  if (Coptind < argc) {
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }

  sprintf (retryenv, "%s=0", CNS_CONRETRY_ENV);
  putenv (retryenv);
  int i;
  for (i=0; i<repetitions; i++){
    ping(server);
  }
  exit (0);
}
