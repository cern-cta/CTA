#include <Cthread_api.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <serrno.h>
#include <osdep.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <signal.h>

#ifndef _WIN32
/* Signal handler - Simplify the POSIX sigaction calls */
#ifdef __STDC__
typedef void    Sigfunc(int);
Sigfunc *_test_signal(int, Sigfunc *);
#else
typedef void    Sigfunc();
Sigfunc *_test_signal();
#endif
#else
#define _test_signal(a,b) signal(a,b)
#endif

void *doit _PROTO((void *));
void reaper _PROTO((int));

static int okstatus = 0;
int n;

int main(argc,argv)
     int argc;
     char **argv;
{
  int nj, nd, signo, i;
  int *cid;

  if (argc != 4) {
    fprintf(stderr,"Usage: %s <nb_of_threads> <nb_of_detachable_threads> <signal_number>\n", argv[0]);
    exit(1);
  }

  nj = atoi(argv[1]);
  nd = atoi(argv[2]);

  if ((n = (nj + nd)) <= 0) {
    fprintf(stderr,"Total number of thread has to be > 0\n");
    exit(1);
  }

  if ((signo = atoi(argv[3])) < 0) {
    fprintf(stderr,"Third argument <signal_number> has to be >= 0\n");
    exit(1);
  }

  fprintf(stderr,"[Main] Masking signo %d in master\n", signo);
  _test_signal(signo,&reaper);

  if ((cid = (int *) malloc(n * sizeof(int))) == NULL) {
    fprintf(stderr,"[Main] malloc error : %s\n", strerror(errno));
    exit(1);
  }

  /* Create n threads */
  for (i = 0; i < n; i++) {
    cid[i] = (i < nj) ? Cthread_create(&doit,NULL) : Cthread_create_detached(&doit,NULL);
    fprintf(stderr,"[Main] Created %s Thread No %d\n", (i < nj) ? "joinable" : "detachable", cid[i]);
  }

  /* Kill the threads */
  for (i = 0; i < n; i++) {
    fprintf(stderr,"[Main] sleeping 1 second\n");
    usleep(1000000);
    fprintf(stderr,"[Main] Signalling Thread No %d, signo=%d\n", cid[i], signo);
    if (Cthread_kill(cid[i],signo) != 0) {
      fprintf(stderr,"[Main] Cthread_kill error on Thread No %d, %s\n", cid[i], sstrerror(serrno));
    }
  }

  /* Joining nj threads */
  for (i = 0; i < n; i++) {
    int *status;

    if (Cthread_join(cid[i],&status) != 0) {
      fprintf(stderr,"[Main] Cthread_join error on Thread No %d, %s\n", cid[i], sstrerror(serrno));
    } else {
      fprintf(stderr,"[Main] Thread No %d has exited with status %d\n", cid[i], *status);
    }
  }

  exit(0);
}

void *doit(arg)
     void *arg;
{
  int cid = Cthread_self();

  fprintf(stderr,"... Thread No %d : sleeping %d seconds\n", cid,2*n);
  usleep(2 * n * 1000000);

  fprintf(stderr,"... Thread No %d : exiting with status 0\n", cid);
  
  return((void *) &okstatus);
}

#ifndef _WIN32
Sigfunc *_test_signal(signo, func)
     int signo;
     Sigfunc *func;
{
  struct sigaction	act, oact;
  int n = 0;

  act.sa_handler = func;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
  } else {
#ifdef	SA_RESTART
    act.sa_flags |= SA_RESTART;		/* SVR4, 44BSD */
#endif
  }
  n = sigaction(signo, &act, &oact);
  if (n < 0) {
    return(SIG_ERR);
  }
  return(oact.sa_handler);
}
#endif /* #ifndef _WIN32 */

void reaper(signo)
     int signo;
{
	/* Cthread_exit((void *) &signo); */
}
