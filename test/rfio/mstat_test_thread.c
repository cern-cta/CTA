#include <Cthread_api.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <serrno.h>

void *doit _PROTO((void *));

#define NTHREADS 2

#define FILE1 "castor5:/tmp/file1"
#define FILE2 "castor5:/tmp/file2"

int main()
{
  int i;

  putenv("RFIO_TRACE=3");

  for (i = 0; i < NTHREADS; i++) {
    Cthread_create(&doit, NULL);
    sleep(1);
  }
  sleep(10);
  exit(0);
}

void *doit(arg)
     void *arg;
{
  char *file1, *file2;
  struct stat statbuf;

  fprintf(stdout, "[%d] Thread started\n", Cthread_self());
  fflush(stdout);
  fprintf(stdout,"We are pid=%d, ppid=%d\n", getpid(), getppid());
  fflush(stdout);
  if ((Cthread_self() % 2) == 0) {
    fprintf(stdout, "[%d] Will use FILE1 and FILE2\n", Cthread_self());
    fflush(stdout);
    file1 = FILE1;
    file2 = FILE2;
  } else {
    fprintf(stdout, "[%d] Will use FILE2 and FILE1\n", Cthread_self());
    fflush(stdout);
    file1 = FILE2;
    file2 = FILE1;
  }
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", Cthread_self(), file1);
  fflush(stdout);
  rfio_mstat(file1,&statbuf);
  sleep(1);
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", Cthread_self(), file2);
  fflush(stdout);
  rfio_mstat(file2,&statbuf);
  sleep(1);
  if ((Cthread_self() % 2) == 0) {
    fprintf(stdout, "[%d] Will now use FILE2 and FILE1\n", Cthread_self());
    fflush(stdout);
    file1 = FILE2;
    file2 = FILE1;
  } else {
    fprintf(stdout, "[%d] Will now use FILE1 and FILE2\n", Cthread_self());
    fflush(stdout);
    file1 = FILE1;
    file2 = FILE2;
  }
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", Cthread_self(), file1);
  fflush(stdout);
  rfio_mstat(file1,&statbuf);
  sleep(1);
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", Cthread_self(), file2);
  fflush(stdout);
  rfio_mstat(file2,&statbuf);
  sleep(1);
  if ((Cthread_self() % 2) == 0) {
    fprintf(stdout, "[%d] Will finally use again FILE1 and FILE2\n", Cthread_self());
    fflush(stdout);
    file1 = FILE1;
    file2 = FILE2;
  } else {
    fprintf(stdout, "[%d] Will finally use again FILE2 and FILE1\n", Cthread_self());
    fflush(stdout);
    file1 = FILE2;
    file2 = FILE1;
  }
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", Cthread_self(), file1);
  fflush(stdout);
  rfio_mstat(file1,&statbuf);
  sleep(1);
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", Cthread_self(), file2);
  fflush(stdout);
  rfio_mstat(file2,&statbuf);
  sleep(1);
  rfio_end();
  return(0);
}
