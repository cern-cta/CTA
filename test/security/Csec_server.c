
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include "nw.h"
#define _THREAD_SAFE
#include <Csec_api.h>

static  FILE *log;



static int process_request(int s);

/*
 * Signal handler
 */
void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        printf("%d> SIGINT or SIGTERM received, exiting !\n", getpid());
        exit(0);
    } else if (sig == SIGPIPE) {
        printf("%d> SIGPIPE received !\n", getpid());
    } else if (sig == SIGCHLD) {
        int rc = 1;
        printf("%d> SIGCHLD received, waiting for zombies !\n", getpid());
        while ((rc = waitpid(-1, NULL, WNOHANG)) > 0) {
            printf("%d> child %d exited!\n", getpid(), rc);
        };
    }
}


void signal_pipe(int sig) {
    if (sig == SIGPIPE) {
        printf("%d> SIGPIPE received !\n", getpid());
        exit(1);
    }
}



int
main(argc, argv)
    int argc;
    char **argv;
{
    u_short port = 1975;
    int s;
    int stmp;
    struct sockaddr_in cliaddr;
    socklen_t len;
    int childpid;
   

    log = stderr;
    
    signal(SIGPIPE, signal_handler);
    signal(SIGCHLD, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);

    
    if (log) fprintf(log, "Starting server\n");
    
    if ((stmp = nw_create_socket(port)) >= 0) {
        do {
            char buff[50];
            
            len = sizeof(cliaddr);
            
            /* Accept a TCP connection */
            if ((s = accept(stmp, (struct  sockaddr  *)&cliaddr, &len)) < 0) {
                perror("accepting connection");
                continue;
            }
            
            if (log) fprintf(log, "Connection from %s:%d\n",
                   inet_ntop(AF_INET, &cliaddr.sin_addr, buff, sizeof(buff)),
                   ntohs(cliaddr.sin_port));

            /*  if (( childpid = fork()) == 0) {  */
/*                  close(stmp); */
/*                  exit(process_request(s)); */
/*              } */

                

            process_request(s); 

            
        } while (1);       
        close(stmp);
    }
    return 0;
}


#define BUF_SIZE 2000

static int process_request(int s) {

    char buf[BUF_SIZE+1];
    int rc;
    int i =0;
    Csec_context_t sec_ctx;
    char buf2[BUF_SIZE+1];

  /*   while (1) { */
/*     Csec_seterrbuf(buf2, BUF_SIZE); */
    Csec_server_initContext(&sec_ctx, CSEC_SERVICE_TYPE_HOST, NULL);
    Csec_server_setSecurityOpts(&sec_ctx, 0); //CSEC_OPT_DELEG_FLAG);
      
    /*   if (Csec_acquireCreds(&sec_ctx) < 0) { */
/*         fprintf(stderr, "Could not acquire credentials !\n"); */
/*         fprintf(stderr, "<%s>\n", Csec_getErrorMessage()); */
/*         return -1; */
/*       } */
      
      if (Csec_server_establishContext(&sec_ctx,
					s) < 0){
        if (log) fprintf(log, "Could not establish context !\n");
        fprintf(stderr, "<%s>\n", Csec_getErrorMessage());
        return -1;
      }
  
      
      /* Checking the VOMS extensions */
      
      {
        int nbfqan = 0, i;
        char **fqan = NULL;
        if (log) fprintf(log, "VO: %s\n", 
                         Csec_server_get_client_vo(&sec_ctx));
        
        fqan = Csec_server_get_client_fqans(&sec_ctx, &nbfqan);
        for(i=0; i<nbfqan; i++) {
          if (log) fprintf(log, "FQAN[%d]: %s\n", 
                           i,fqan[i]);
        }
      }
      


    
      { 
	char *mech, *name, *amech, *aname;
	char *dcred;
	int dcredlen;
	char *dcredmech;
	uid_t uid;
	gid_t gid;

/* 	Csec_server_getDelegatedCredential(&sec_ctx,  */
/* 					   &dcredmech, */
/* 					   &dcred, */
/* 					   &dcredlen); */

/* 	{ */
/* 	  int fd; */
/* 	  fd = open("/tmp/dcred.ben", O_CREAT | O_TRUNC | O_WRONLY, 0666); */
/* 	  write(fd, dcred, dcredlen); */
/* 	  close(fd); */
/* 	}	 */


  

	Csec_server_getClientId(&sec_ctx, &mech, &name);
	if (log) fprintf(log, "Received connection from: mechanism='%s' name='%s'\n", 
			 mech, name);
	
	Csec_server_getAuthorizationId(&sec_ctx, &amech, &aname);
	if (log) fprintf(log, "Authorization ID is: %s/%s\n", 
			 amech, aname);
		
	if (Csec_mapToLocalUser (mech, 
			     name, 
			     buf, 
			     BUF_SIZE, 
			     &uid, 
			     &gid) != 0) {
	  fprintf(log, "cannot map into local user!\n");
	  return -1;
	}
	
	if (log) fprintf(log, "%s/%s mapped to %s\n", 
			 mech, name, buf);

      }

      Csec_clearContext(&sec_ctx);
    /*   }  */
    return 0;
}











