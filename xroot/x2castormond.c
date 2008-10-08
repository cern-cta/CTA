#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char* argv[]) {
  if (argc !=4) {
    fprintf(stderr,"ERROR: missing perl script & log file name to run: x2castormond <script> <procdir> <logfile>\n");
    exit(-1);
  }


  pid_t m_pid=fork();
  if(m_pid<0) {
    fprintf(stderr,"ERROR: Failed to fork daemon process\n");
    exit(-1);
  }
  
  // kill the parent
  if(m_pid>0)
    exit(0);

  // reopen stdout to the logfile name
  FILE* fstdout;
  
  if ((!(fstdout=freopen(argv[3], "w", stdout)))) {
    fprintf(stderr,"ERROR: cannot stream stdout into %s\n",argv[3]);
    exit(-1);
  }

  setvbuf(fstdout, (char *)NULL, _IONBF, 0);
  
  FILE* fstderr;
  if ((!(fstderr=freopen(argv[3], "a+", stderr)))) {
    fprintf(stderr,"ERROR: cannot stream stderr into %s\n",argv[3]);
    exit(-1);
  }
  
  setvbuf(fstderr, (char *)NULL, _IONBF, 0);

  pid_t sid;
  if((sid=setsid()) < 0) {
    fprintf(stderr,"ERROR: failed to create new session (setsid())\n");
    exit(-1);
  }
  execlp("/usr/bin/perl",argv[0],argv[1],argv[2],NULL);
}
