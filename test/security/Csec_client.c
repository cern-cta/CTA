#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "nw.h"
#include <Csec_api.h>

#define BUF_SIZE 100

FILE* log;


int main(argc, argv)
    int argc;
    char *argv[];
{
  int s, rc, i,  nbauths = 1;
  Csec_context_t sec_ctx;
  
  
  log = stderr;
  
  if (argc <2) {
    fprintf(stderr, "Please specify the server IP address\n");
    exit(1);
  }
  
  if (argc == 3) {
    nbauths = atoi(argv[2]);
  }


  if (log) fprintf(log, "Connecting to server: %s\n", argv[1]);
  s = nw_connect_to_server(argv[1], 1975);
  if (s < 0) {
    fprintf(stderr, "Could not connect to server !\n");
    return -1;
  }

  
  for(i=1; i<=nbauths; i++) {
    
    Csec_client_initContext(&sec_ctx,CSEC_SERVICE_TYPE_HOST, NULL);
    Csec_client_setSecurityOpts(&sec_ctx, CSEC_OPT_DELEG_FLAG);
    // Csec_client_setAuthorizationId(&sec_ctx, "GSI", "totototototo");

    if (log) fprintf(log, "<%d> Establishing context\n", i);    

    if ( (rc= Csec_client_establishContext(&sec_ctx, s))<0) {
      fprintf(stderr, "Could not establish context: <%s> !\n",
	      Csec_getErrorMessage());
        exit(1);
    }

    printf("<%d> Context established ok\n", i);

    printf("<%d> Context established ok with: %s\n", 
           i, Csec_client_get_service_name(&sec_ctx));

    Csec_clearContext(&sec_ctx);

  }
    close(s);
    return 0;

}




