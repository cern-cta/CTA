#include <stdlib.h>
#include <stdio.h>
#include "serrno.h"
#include "Csetprocname.h"

#define MYMSG "\n\n--> Look to process table, you should see '%s: %s'\n (with blanks after)\n\n"


int main(argc,argv,envp)
     int argc;
     char **argv;
     char **envp;
{
  char savargv0[1024];
  char savargv1[1024];
  char savargv2[1024];
  char savargv3[1024];

  if (argc != 2 && argc != 3 && argc != 4) {
    fprintf(stderr,"Usage: %s <New program name> [<Next1> [<Next2>]] &\n\n... The '&' is to allow you to do 'ps'. Then look to process table!\n", argv[0]);
    exit(1);
  }

  if (Cinitsetprocname(argc, argv, envp) != 0) {
    fprintf(stderr,"### Cinitsetprocname error, serrno=%d (%s)\n", serrno, sstrerror(serrno));
    exit(1);
  }

  strcpy(savargv0, argv[0]);
  strcpy(savargv1, argv[1]);
  if (argc >= 3) strcpy(savargv2, argv[2]);
  if (argc >= 4) strcpy(savargv3, argv[3]);

  if (Csetprocname("%s: %s", savargv0, savargv1) != 0) {
    fprintf(stderr,"### Csetprocname error, serrno=%d (%s)\n", serrno, sstrerror(serrno));
    exit(1);
  }

  fprintf(stderr, MYMSG, savargv0, savargv1);
  fprintf(stderr,"... Sleeping 5 seconds\n");
  sleep(5);
  if (argc >= 3) {
    if (Csetprocname("%s: %s", savargv0, savargv2) != 0) {
      fprintf(stderr,"### Csetprocname error, serrno=%d (%s)\n", serrno, sstrerror(serrno));
      exit(1);
    }
    
    fprintf(stderr, MYMSG, savargv0, savargv2);
    fprintf(stderr,"... Sleeping 5 seconds\n");
    sleep(5);
  }
  if (argc >= 4) {
    if (Csetprocname("%s: %s", savargv0, savargv3) != 0) {
      fprintf(stderr,"### Csetprocname error, serrno=%d (%s)\n", serrno, sstrerror(serrno));
      exit(1);
    }
    
    fprintf(stderr, MYMSG, savargv0, savargv3);
    fprintf(stderr,"... Sleeping 5 seconds\n");
    sleep(5);
  }

  exit(0);

}
