#include <Cthread_api.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <serrno.h>
#include <unistd.h>

void *doit (void *);

#define NFORK 2

#define FILE1 "castor5:/tmp/file1"
#define FILE2 "castor5:/tmp/file2"

int main()
{
  int i;
  struct stat statbuf;

  putenv("RFIO_TRACE=3");

  fprintf(stdout, "[%d] Pre-fill multiple connection tab in parent started\n", getpid());
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), FILE1);
  fflush(stdout);
  rfio_mstat(FILE1,&statbuf);
  sleep(1);
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), FILE2);
  fflush(stdout);
  rfio_mstat(FILE2,&statbuf);
  sleep(1);

  for (i = 0; i < NFORK; i++) {
    if (fork() == 0) {
      doit(NULL);
      exit(0);
    }
  }
  sleep(10);
  rfio_end();
  exit(0);
}

void *doit(arg)
     void *arg;
{
  char *file1, *file2;
  struct stat statbuf;

  fprintf(stdout, "[%d] Process started\n", getpid());
  fflush(stdout);
  if ((getpid() % 2) == 0) {
    fprintf(stdout, "[%d] Will use FILE1 and FILE2\n", getpid());
    fflush(stdout);
    file1 = FILE1;
    file2 = FILE2;
  } else {
    fprintf(stdout, "[%d] Will use FILE2 and FILE1\n", getpid());
    fflush(stdout);
    file1 = FILE2;
    file2 = FILE1;
  }
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), file1);
  fflush(stdout);
  rfio_mstat(file1,&statbuf);
  sleep(1);
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), file2);
  fflush(stdout);
  rfio_mstat(file2,&statbuf);
  sleep(1);
  if ((getpid() % 2) == 0) {
    fprintf(stdout, "[%d] Will now use FILE2 and FILE1\n", getpid());
    fflush(stdout);
    file1 = FILE2;
    file2 = FILE1;
  } else {
    fprintf(stdout, "[%d] Will now use FILE1 and FILE2\n", getpid());
    fflush(stdout);
    file1 = FILE1;
    file2 = FILE2;
  }
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), file1);
  fflush(stdout);
  rfio_mstat(file1,&statbuf);
  sleep(1);
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), file2);
  fflush(stdout);
  rfio_mstat(file2,&statbuf);
  sleep(1);
  if ((getpid() % 2) == 0) {
    fprintf(stdout, "[%d] Will finally use again FILE1 and FILE2\n", getpid());
    fflush(stdout);
    file1 = FILE1;
    file2 = FILE2;
  } else {
    fprintf(stdout, "[%d] Will finally use again FILE2 and FILE1\n", getpid());
    fflush(stdout);
    file1 = FILE2;
    file2 = FILE1;
  }
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), file1);
  fflush(stdout);
  rfio_mstat(file1,&statbuf);
  sleep(1);
  fprintf(stdout, "[%d] rfio_mstat(\"%s\")\n", getpid(), file2);
  fflush(stdout);
  rfio_mstat(file2,&statbuf);
  sleep(1);
  rfio_end();
  return(0);
}
