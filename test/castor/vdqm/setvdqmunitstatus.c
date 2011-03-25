#include <sys/types.h>
#include <unistd.h>
#include "h/net.h"
#include "h/Castor_limits.h"
#include "h/vdqm_api.h"

#include <stdio.h>
#include <string.h>

void printUsgae(FILE *stream) {
  fprintf(stream, "Usage:\n");
  fprintf(stream, "\tsetvdqmunitstatus dgn drive status jobID\n");
}

int main(int argc, char **argv) {
  char *dgn   = NULL;
  char *drive = NULL;
  int  status = VDQM_UNIT_DOWN;
  int  jobID  = 0;
  int  rc     = 0;

  if(argc != 5) {
    fprintf(stderr, "Wrong number of command-line arguments\n");
    printUsgae(stderr);
    return (1);
  }

  dgn    = argv[1];
  drive  = argv[2];
  status = atoi(argv[3]);
  jobID  = atoi(argv[4]);

  printf("Calling vdqm_UnitStatus:"
    " dgn=%s, drive=%s, vdqm_status=%d, jobID=%d\n",
    dgn, drive, status, jobID);
  rc = vdqm_UnitStatus(NULL, NULL, dgn, NULL, drive, &status, NULL, jobID);

  printf("Value returned by vdqm_UnitStatus = %d\n", rc);

  return 0;
}
