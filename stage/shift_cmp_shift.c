/*
 * $Id: shift_cmp_shift.c,v 1.3 2000/01/26 18:30:55 jdurand Exp $
 */

/* ============== */
/* System headers */
/* ============== */
#include <stdio.h>            /* All I/O defs */
#include <stdlib.h>           /* malloc etc... */
#include <sys/stat.h>         /* fstat etc... */
#include <sys/types.h>        /* For all types  */
#include <fcntl.h>            /* RDWR etc... */
#include <unistd.h>           /* Getopt etc... */
#include <stdlib.h>           /* atoi etc... */
#include <errno.h>            /* errno etc... */
#include <string.h>           /* memset etc... */

/* ============= */
/* Local headers */
/* ============= */
#include "osdep.h"            /* For _PROTO */
#include "stage_shift.h"
#include "u64subr.h"

/* ====== */
/* Macros */
/* ====== */
#ifdef _WIN32
#define FILE_OFLAG ( O_RDONLY | O_BINARY )
#else
#define FILE_OFLAG O_RDONLY
#endif
#ifdef _WIN32
#define FILE_MODE ( _S_IREAD | _S_IWRITE )
#else
#define FILE_MODE ( S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH )
#endif
#define FREQUENCY 1000

#ifndef MIN
#define MIN(x,y) ((x) < (y) ? (x) : (y))
#endif

#define STCCMP_VAL(member) {                                                    \
  printf("%10s : %10d ... %10d ... %10s ... %10s\n", #member ,                  \
         (int) stcp1->member,(int) stcp2->member,                               \
         stcp1->member == stcp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stcp1->member != stcp2->member || memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog1:\n", #member );                     \
    shift_cmp_shift_dump((char *) &(stcp1->member),sizeof(stcp1->member));           \
    printf("... Dump of %s entry in catalog2:\n", #member );                         \
    shift_cmp_shift_dump((char *) &(stcp2->member),sizeof(stcp2->member));           \
  }                                                                             \
}

#define STCCMP_CHAR(member) {                                                   \
  printf("%10s : %10c ... %10c ... %10s ... %10s\n", #member ,                  \
         stcp1->member != '\0' ? stcp1->member : ' ',stcp2->member != '\0' ? stcp2->member : ' ',                 \
         stcp1->member == stcp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stcp1->member != stcp2->member || memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog1:\n", #member );                     \
    shift_cmp_shift_dump(&(stcp1->member),sizeof(stcp1->member));                    \
    printf("... Dump of %s entry in catalog2:\n", #member );                         \
    shift_cmp_shift_dump(&(stcp2->member),sizeof(stcp2->member));                    \
  }                                                                             \
}

#define STCCMP_STRING(member) {                                                 \
  printf("%10s : %10s ... %10s ... %10s ... %10s\n", #member ,                  \
         stcp1->member,stcp2->member,                                           \
         strcmp(stcp1->member,stcp2->member) == 0 ? "OK" : "NOT OK",            \
         memcmp(stcp1->member,stcp2->member,sizeof(stcp1->member)) == 0 ?       \
         "OK" : "NOT OK");                                                      \
  if (strcmp(stcp1->member,stcp2->member) != 0 || memcmp(stcp1->member,stcp2->member,sizeof(stcp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog1:\n", #member );                     \
    shift_cmp_shift_dump(stcp1->member,sizeof(stcp1->member));                       \
    printf("... Dump of %s entry in catalog2:\n", #member );                         \
    shift_cmp_shift_dump(stcp2->member,sizeof(stcp2->member));                       \
  }                                                                             \
}

#define STPCMP_VAL(member) {                                                    \
  printf("%10s : %10d ... %10d ... %10s ... %10s\n", #member ,                  \
         (int) stpp1->member,(int) stpp2->member,                               \
         stpp1->member == stpp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stpp1->member != stpp2->member || memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog1:\n", #member );                     \
    shift_cmp_shift_dump((char *) &(stpp1->member),sizeof(stpp1->member));           \
    printf("... Dump of %s entry in catalog2:\n", #member );                         \
    shift_cmp_shift_dump((char *) &(stpp2->member),sizeof(stpp2->member));           \
  }                                                                             \
}

#define STPCMP_CHAR(member) {                                                   \
  printf("%10s : %10c ... %10c ... %10s ... %10s\n", #member ,                  \
         stpp1->member != '\0' ? stpp1->member : ' ',stpp2->member != '\0' ? stpp2->member : ' ',                 \
         stpp1->member == stpp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stpp1->member != stpp2->member || memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog1:\n", #member );                     \
    shift_cmp_shift_dump(&(stpp1->member),sizeof(stpp1->member));                    \
    printf("... Dump of %s entry in catalog2:\n", #member );                         \
    shift_cmp_shift_dump(&(stpp2->member),sizeof(stpp2->member));                    \
  }                                                                             \
}

#define STPCMP_STRING(member) {                                                 \
  printf("%10s : %10s ... %10s ... %10s ... %10s\n", #member ,                  \
         stpp1->member,stpp2->member,                                           \
         strcmp(stpp1->member,stpp2->member) == 0 ? "OK" : "NOT OK",            \
         memcmp(stpp1->member,stpp2->member,sizeof(stpp1->member)) == 0 ?       \
         "OK" : "NOT OK");                                                      \
  if (strcmp(stpp1->member,stpp2->member) != 0 || memcmp(stpp1->member,stpp2->member,sizeof(stpp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog1:\n", #member );                     \
    shift_cmp_shift_dump(stpp1->member,sizeof(stpp1->member));                       \
    printf("... Dump of %s entry in catalog2:\n", #member );                         \
    shift_cmp_shift_dump(stpp2->member,sizeof(stpp2->member));                       \
  }                                                                             \
}

/* =============== */
/* Local variables */
/* =============== */
char *stgcat_in1, *stgpath_in1, *stgcat_in2, *stgpath_in2;
int stgcat_in1_fd, stgpath_in1_fd, stgcat_in2_fd, stgpath_in2_fd;
struct stat stgcat_in1_statbuff, stgpath_in1_statbuff, stgcat_in2_statbuff, stgpath_in2_statbuff;
int frequency = FREQUENCY;
int bindiff = 0;

/* ========== */
/* Prototypes */
/* ========== */
void shift_cmp_shift_usage _PROTO(());
int cmp_stgcat _PROTO(());
int cmp_stgpath _PROTO(());
int stcpcmp _PROTO((struct stgcat_entry_old *, struct stgcat_entry_old *, int));
int stppcmp _PROTO((struct stgpath_entry_old *, struct stgpath_entry_old *, int));
void stcpprint _PROTO((struct stgcat_entry_old *, struct stgcat_entry_old *));
void stppprint _PROTO((struct stgpath_entry_old *, struct stgpath_entry_old *));
void shift_cmp_shift_dump _PROTO((char *, unsigned int));
void shift_cmp_shift_dump2 _PROTO((char *, char *, unsigned int));

int main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  int help = 0;
  int skip_stgcat = 0;
  int skip_stgpath = 0;
  extern char *optarg;
  extern int optind, opterr, optopt;
  int errflg = 0;
  char tmpbuf[21];
  int answer;

  while ((c = getopt(argc,argv,"bhCLn:")) != EOF) {
    switch (c) {
    case 'b':
      bindiff = 1;
      break;
    case 'h':
      help = 1;
      break;
    case 'C':
      skip_stgcat = 1;
      break;
    case 'L':
      skip_stgpath = 1;
      break;
    case 'n':
      frequency = atoi(optarg);
      break;
    case '?':
      ++errflg;
      printf("Unknown option\n");
      break;
    default:
      ++errflg;
      printf("?? getopt returned character code 0%o (octal) 0x%lx (hex) %d (int) '%c' (char) ?\n"
             ,c,(unsigned long) c,c,(char) c);
      break;
    }
  }

  if (errflg != 0) {
    printf("### getopt error\n");
    shift_cmp_shift_usage();
    return(EXIT_FAILURE);
  }

  if (help != 0) {
    shift_cmp_shift_usage();
    return(EXIT_SUCCESS);
  }

  if (optind >= argc || optind > (argc - 4)) {
    printf("?? Exactly four parameters are requested\n");
    shift_cmp_shift_usage();
    return(EXIT_FAILURE);
  }

  stgcat_in1 = argv[optind];
  stgpath_in1 = argv[optind+1];
  stgcat_in2 = argv[optind+2];
  stgpath_in2 = argv[optind+3];

  if (skip_stgcat == 0) {
    if ((stgcat_in1_fd = open(stgcat_in1, FILE_OFLAG, FILE_MODE)) < 0) {
      printf("### open of %s error, %s\n",stgcat_in1,strerror(errno));
      return(EXIT_FAILURE);      
    }
    if ((stgcat_in2_fd = open(stgcat_in2, FILE_OFLAG, FILE_MODE)) < 0) {
      printf("### open of %s error, %s\n",stgcat_in2,strerror(errno));
      return(EXIT_FAILURE);      
    }
    /* Check the sizes */
    if (fstat(stgcat_in1_fd, &stgcat_in1_statbuff) < 0) {
      printf("### fstat on %s error, %s\n"
             ,stgcat_in1
             ,strerror(errno));
      return(EXIT_FAILURE);
    }
    /* - stgcat_in1 has to be length > 0 */
    if (stgcat_in1_statbuff.st_size <= 0) {
      printf("### %s is of zero size !\n"
             ,stgcat_in1);
      return(EXIT_FAILURE);
    }
    if (fstat(stgcat_in2_fd, &stgcat_in2_statbuff) < 0) {
      printf("### fstat on %s error, %s\n"
             ,stgcat_in2
             ,strerror(errno));
      return(EXIT_FAILURE);
    }
    /* - stgcat_in2 has to be length > 0 (asking to proceed anyway otherwise) */
    if (stgcat_in2_statbuff.st_size <= 0) {
      printf("### %s is of zero size !\n"
             ,stgcat_in2);
      return(EXIT_FAILURE);
    }
  }
  if (skip_stgpath == 0) {
    if ((stgpath_in1_fd = open(stgpath_in1, FILE_OFLAG, FILE_MODE)) < 0) {
      printf("### open of %s error, %s\n",stgpath_in1,strerror(errno));
      return(EXIT_FAILURE);      
    }
    if ((stgpath_in2_fd = open(stgpath_in2, FILE_OFLAG, FILE_MODE)) < 0) {
      printf("### open of %s error, %s\n",stgpath_in2,strerror(errno));
      return(EXIT_FAILURE);      
    }
    /* Check the sizes */
    if (fstat(stgpath_in1_fd, &stgpath_in1_statbuff) < 0) {
      printf("### fstat on %s error, %s\n"
             ,stgpath_in1
             ,strerror(errno));
      return(EXIT_FAILURE);
    }
    /* - stgpath_in1 has to be length > 0 */
    if (stgpath_in1_statbuff.st_size <= 0) {
      printf("### %s is of zero size !\n"
             ,stgpath_in1);
      return(EXIT_FAILURE);
    }
    if (fstat(stgpath_in2_fd, &stgpath_in2_statbuff) < 0) {
      printf("### fstat on %s error, %s\n"
             ,stgpath_in2
             ,strerror(errno));
      return(EXIT_FAILURE);
    }
    /* - stgpath_in2 has to be length > 0 */
    if (stgpath_in2_statbuff.st_size <= 0) {
      printf("### %s is of zero size !\n"
             ,stgpath_in2);
      return(EXIT_FAILURE);
    }
  }

  if (skip_stgcat == 0) {
    if (cmp_stgcat() != 0) {
      return(EXIT_FAILURE);
    }
    close(stgcat_in1_fd);
    close(stgcat_in2_fd);
  }
  if (skip_stgpath == 0) {
    if (cmp_stgpath() != 0) {
      return(EXIT_FAILURE);
    }
    close(stgpath_in1_fd);
    close(stgpath_in2_fd);
  }

  return(EXIT_SUCCESS);
}

int cmp_stgcat() {
  int answer;
  struct stgcat_entry_old stcp1;
  struct stgcat_entry_old *stcp1_all = NULL;
  struct stgcat_entry_old stcp2;
  struct stgcat_entry_old *stcp2_all = NULL;
  int i;
  int j;
  int found;
  int imax;
  int jmax;

  /* We check in the size of the in-file module SHIFT stager structure matches... */
  if (stgcat_in1_statbuff.st_size % sizeof(struct stgcat_entry_old) != 0) {
    printf("### Current %s length %% sizeof(old SHIFT structure) is NOT zero (%d)...\n"
           ".... I believe that either this file is truncated or currupted, either\n"
           ".... you did not gave a read old SHIFT stager stgcat file as input\n"
           ,stgcat_in1,
           (int) (stgcat_in1_statbuff.st_size % sizeof(struct stgcat_entry_old)));
    printf("### Do you really want to proceed (y/n) ? ");
    answer = getchar();
    fflush(stdin);
    if (answer != 'y' && answer != 'Y') {
      return(-1);
    }
  }
  if (stgcat_in2_statbuff.st_size % sizeof(struct stgcat_entry_old) != 0) {
    printf("### Current %s length %% sizeof(old SHIFT structure) is NOT zero (%d)...\n"
           ".... I believe that either this file is truncated or currupted, either\n"
           ".... you did not gave a read old SHIFT stager stgcat file as input\n"
           ,stgcat_in2,
           (int) (stgcat_in2_statbuff.st_size % sizeof(struct stgcat_entry_old)));
    printf("### Do you really want to proceed (y/n) ? ");
    answer = getchar();
    fflush(stdin);
    if (answer != 'y' && answer != 'Y') {
      return(-1);
    }
  }
  if (stgcat_in1_statbuff.st_size != stgcat_in2_statbuff.st_size) {
    printf("### Current %s and %s lengths are NOT the same (%d and %d)...\n"
           ".... I believe that either this file is truncated or currupted, either\n"
           ".... you did not gave a read old SHIFT stager stgcat file as input\n"
           ,stgcat_in1,stgcat_in2,
           (int) stgcat_in1_statbuff.st_size,
           (int) stgcat_in2_statbuff.st_size);
    printf("### Do you really want to proceed (y/n) ? ");
    answer = getchar();
    fflush(stdin);
    if (answer != 'y' && answer != 'Y') {
      return(-1);
    }
  }

  /* We load the files in memory */
  if ((stcp1_all = (struct stgcat_entry_old *) malloc(stgcat_in1_statbuff.st_size)) == NULL) {
    printf("### malloc error on %s (%s) at %s:%d\n",stgcat_in1,
           strerror(errno),__FILE__,__LINE__);
    return(-1);
  }
  if ((stcp2_all = (struct stgcat_entry_old *) malloc(stgcat_in2_statbuff.st_size)) == NULL) {
    printf("### malloc error on %s (%s) at %s:%d\n",stgcat_in1,
           strerror(errno),__FILE__,__LINE__);
    free(stcp1_all);
    return(-1);
  }
  if (lseek(stgcat_in1_fd,SEEK_SET,0) != 0) {
    printf("### lseek error on %s (%s) at %s:%d\n",stgcat_in2,
           strerror(errno),__FILE__,__LINE__);
    free(stcp1_all);
    free(stcp2_all);
    return(-1);
  }
  printf("... Loading %s\n",stgcat_in1);
  if (read(stgcat_in1_fd,stcp1_all,stgcat_in1_statbuff.st_size) != stgcat_in1_statbuff.st_size) {
    printf("### read error on %s (%s) at %s:%d\n",stgcat_in1,
           strerror(errno),__FILE__,__LINE__);
    free(stcp1_all);
    free(stcp2_all);
    return(-1);
  }
  printf("... Loading %s\n",stgcat_in2);
  if (read(stgcat_in2_fd,stcp2_all,stgcat_in2_statbuff.st_size) != stgcat_in2_statbuff.st_size) {
    printf("### read error on %s (%s) at %s:%d\n",stgcat_in2,
           strerror(errno),__FILE__,__LINE__);
    free(stcp1_all);
    free(stcp2_all);
    return(-1);
  }

  /* We compare all the entries using file 1 as a reference */
  imax = stgcat_in1_statbuff.st_size / sizeof(struct stgcat_entry_old) - 1;
  jmax = stgcat_in2_statbuff.st_size / sizeof(struct stgcat_entry_old) - 1;

  printf("\n--> Use %s as a reference\n\n",stgcat_in1);

  for (i = 0; i <= imax; i++) {
    found = -1;
    stcp1 = stcp1_all[i];
    for (j = 0; j <= jmax; j++) {
      stcp2 = stcp2_all[j];
      if (stcp2.reqid == stcp1.reqid) {
        found = j;
        break;
      }
    }
    if (found < 0) {
      printf("### (%5d/%5d) Reqid = %d (%s) not in %s\n",i,imax,
             stcp1.reqid,stgcat_in1,stgcat_in2);
    }
    stcp2 = stcp2_all[found];
    /* Compare the SHIFT entries */
    if (i == 0 || i % frequency == 0 || i == imax) {
      printf("--> (%5d/%5d) Doing reqid = %d / %d\n",i,imax,stcp1.reqid,stcp2.reqid);
    }
    if (stcpcmp(&stcp1,&stcp2,bindiff) != 0) {
      stcpprint(&stcp1,&stcp2);
    }
  }

  printf("\n--> Use %s as a reference\n\n",stgcat_in2);

  /* We compare all the entries using file 2 as a reference */
  for (j = 0; j <= jmax; j++) {
    found = -1;
    stcp2 = stcp2_all[j];
    for (i = 0; i <= imax; i++) {
      stcp1 = stcp1_all[i];
      if (stcp2.reqid == stcp1.reqid) {
        found = i;
        break;
      }
    }
    if (found < 0) {
      printf("### (%5d/%5d) Reqid = %d (%s) not in %s\n",j,jmax,
             stcp2.reqid,stgcat_in2,stgcat_in1);
    }
    stcp1 = stcp1_all[found];
    /* Compare the SHIFT entries */
    if (j == 0 || j % frequency == 0 || j == jmax) {
      printf("--> (%5d/%5d) Doing reqid = %d / %d\n",j,jmax,stcp2.reqid,stcp1.reqid);
    }
    if (stcpcmp(&stcp2,&stcp1,bindiff) != 0) {
      stcpprint(&stcp2,&stcp1);
    }
  }
  free(stcp1_all);
  free(stcp2_all);
  return(0);
}

int cmp_stgpath() {
  int answer;
  struct stgpath_entry_old stpp1;
  struct stgpath_entry_old *stpp1_all = NULL;
  struct stgpath_entry_old stpp2;
  struct stgpath_entry_old *stpp2_all = NULL;
  int i, i2;
  int j, j2;
  int found;
  int imax;
  int jmax;

  /* We check in the size of the in-file module SHIFT stager structure matches... */
  if (stgpath_in1_statbuff.st_size % sizeof(struct stgpath_entry_old) != 0) {
    printf("### Current %s length %% sizeof(old SHIFT structure) is NOT zero (%d)...\n"
           ".... I believe that either this file is truncated or currupted, either\n"
           ".... you did not gave a read old SHIFT stager stgpath file as input\n"
           ,stgpath_in1,
           (int) (stgpath_in1_statbuff.st_size % sizeof(struct stgpath_entry_old)));
    printf("### Do you really want to proceed (y/n) ? ");
    answer = getchar();
    fflush(stdin);
    if (answer != 'y' && answer != 'Y') {
      return(-1);
    }
  }
  if (stgpath_in2_statbuff.st_size % sizeof(struct stgpath_entry_old) != 0) {
    printf("### Current %s length %% sizeof(old SHIFT structure) is NOT zero (%d)...\n"
           ".... I believe that either this file is truncated or currupted, either\n"
           ".... you did not gave a read old SHIFT stager stgpath file as input\n"
           ,stgpath_in2,
           (int) (stgpath_in2_statbuff.st_size % sizeof(struct stgpath_entry_old)));
    printf("### Do you really want to proceed (y/n) ? ");
    answer = getchar();
    fflush(stdin);
    if (answer != 'y' && answer != 'Y') {
      return(-1);
    }
  }
  if (stgpath_in1_statbuff.st_size != stgpath_in2_statbuff.st_size) {
    printf("### Current %s and %s lengths are NOT the same (%d and %d)...\n"
           ".... I believe that either this file is truncated or currupted, either\n"
           ".... you did not gave a read old SHIFT stager stgpath file as input\n"
           ,stgpath_in1,stgpath_in2,
           (int) stgpath_in1_statbuff.st_size,
           (int) stgpath_in2_statbuff.st_size);
    printf("### Do you really want to proceed (y/n) ? ");
    answer = getchar();
    fflush(stdin);
    if (answer != 'y' && answer != 'Y') {
      return(-1);
    }
  }

  /* We load the files in memory */
  if ((stpp1_all = (struct stgpath_entry_old *) malloc(stgpath_in1_statbuff.st_size)) == NULL) {
    printf("### malloc error on %s (%s) at %s:%d\n",stgpath_in1,
           strerror(errno),__FILE__,__LINE__);
    return(-1);
  }
  if ((stpp2_all = (struct stgpath_entry_old *) malloc(stgpath_in2_statbuff.st_size)) == NULL) {
    printf("### malloc error on %s (%s) at %s:%d\n",stgpath_in1,
           strerror(errno),__FILE__,__LINE__);
    free(stpp1_all);
    return(-1);
  }
  if (lseek(stgpath_in1_fd,SEEK_SET,0) != 0) {
    printf("### lseek error on %s (%s) at %s:%d\n",stgpath_in2,
           strerror(errno),__FILE__,__LINE__);
    free(stpp1_all);
    free(stpp2_all);
    return(-1);
  }
  printf("... Loading %s\n",stgpath_in1);
  if (read(stgpath_in1_fd,stpp1_all,stgpath_in1_statbuff.st_size) != stgpath_in1_statbuff.st_size) {
    printf("### read error on %s (%s) at %s:%d\n",stgpath_in1,
           strerror(errno),__FILE__,__LINE__);
    free(stpp1_all);
    free(stpp2_all);
    return(-1);
  }
  printf("... Loading %s\n",stgpath_in2);
  if (read(stgpath_in2_fd,stpp2_all,stgpath_in2_statbuff.st_size) != stgpath_in2_statbuff.st_size) {
    printf("### read error on %s (%s) at %s:%d\n",stgpath_in2,
           strerror(errno),__FILE__,__LINE__);
    free(stpp1_all);
    free(stpp2_all);
    return(-1);
  }

  /* We compare all the entries using file 1 as a reference */
  imax = stgpath_in1_statbuff.st_size / sizeof(struct stgpath_entry_old) - 1;
  jmax = stgpath_in2_statbuff.st_size / sizeof(struct stgpath_entry_old) - 1;

  printf("\n--> Use %s as a reference\n\n",stgpath_in1);

  for (i = 0; i <= imax; i++) {
    found = -1;
    stpp1 = stpp1_all[i];
    for (j = 0; j <= jmax; j++) {
      stpp2 = stpp2_all[j];
      if (stpp2.reqid == stpp1.reqid) {
        /* We search for the last one... */
        found = j;
        /* break; */
      }
    }
    if (found < 0) {
      printf("### (%5d/%5d) Reqid = %d (%s) not in %s\n",i,imax,
             stpp1.reqid,stgpath_in1,stgpath_in2);
    }
    stpp2 = stpp2_all[found];
    found = -1;
    /* We look if there are other entries in first first with the same reqid...     */
    /* For stgpath we always need to take the LAST of the entries with this reqid ! */
    for (i2 = 0; i2 <= imax; i2++) {
      stpp1 = stpp1_all[i2];
      if (stpp1.reqid == stpp2.reqid) {
        found = i2;
      }
    }
    if (found < 0) {
      printf("### Internal error...\n");
      free(stpp1_all);
      free(stpp2_all);
      return(-1);
    }
    stpp1 = stpp1_all[found];
    /* Compare the SHIFT entries */
    if (i == 0 || i % frequency == 0 || i == imax) {
      printf("--> (%5d/%5d) Doing reqid = %d / %d\n",i,imax,stpp1.reqid,stpp2.reqid);
    }
    if (stppcmp(&stpp1,&stpp2,bindiff) != 0) {
      stppprint(&stpp1,&stpp2);
    }
  }

  printf("\n--> Use %s as a reference\n\n",stgpath_in2);

  /* We compare all the entries using file 2 as a reference */
  for (j = 0; j <= jmax; j++) {
    found = -1;
    stpp2 = stpp2_all[j];
    for (i = 0; i <= imax; i++) {
      stpp1 = stpp1_all[i];
      if (stpp2.reqid == stpp1.reqid) {
        /* We search for the last one... */
        found = i;
        /* break; */
      }
    }
    if (found < 0) {
      printf("### (%5d/%5d) Reqid = %d (%s) not in %s\n",j,jmax,
             stpp2.reqid,stgpath_in2,stgpath_in1);
    }
    stpp1 = stpp1_all[found];
    /* We look if there are other entries in first first with the same reqid...     */
    /* For stgpath we always need to take the LAST of the entries with this reqid ! */
    found = -1;
    for (j2 = 0; j2 <= jmax; j2++) {
      stpp2 = stpp2_all[j2];
      if (stpp2.reqid == stpp1.reqid) {
        found = j2;
      }
    }
    if (found < 0) {
      printf("### Internal error...\n");
      free(stpp1_all);
      free(stpp2_all);
      return(-1);
    }
    stpp2 = stpp2_all[found];
    /* Compare the SHIFT entries */
    if (j == 0 || j % frequency == 0 || j == jmax) {
      printf("--> (%5d/%5d) Doing reqid = %d / %d\n",j,jmax,stpp2.reqid,stpp1.reqid);
    }
    if (stppcmp(&stpp2,&stpp1,bindiff) != 0) {
      stppprint(&stpp2,&stpp1);
    }
  }
  free(stpp1_all);
  free(stpp2_all);
  return(0);
}

void shift_cmp_shift_usage() {
  printf("Usage : stgshift_cmp_shift [options] <stgcat_in1> <stgpath_in2> <stgcat_in2> <stgpath_in2>\n"
         "\n"
         "  where options can be:\n"
         "  -b           Binary diff\n"
         "  -h           This help\n"
         "  -C           Skip stgcat comparison\n"
         "  -L           Skip stgpath comparison\n"
         "  -n <number>  Print output every <number> iterations. Default is %d.\n"
         "\n"
         "  This program will compare two SHIFT stager catalogs. It is dedicated to check that the conversion programs shift_to_castor and castor_to_shift gives the same SHIFT catalog... The SHIFT stager catalogs are typically <stgcat_in> == /usr/spool/stage/stgcat and <stgpath_in> == /usr/spool/stage/stgpath\n"
         "\n"
         "  Example:\n"
         "stgshift_cmp_shift /usr/spool/stage/stgcat /usr/spool/stage/stgpath ./stgcat_shift_bak ./stgpath_shift_bak\n"
         "Comments to castor-support@listbox.cern.ch\n"
         "\n",
         FREQUENCY
         );
}

int stcpcmp(stcp1,stcp2,bindiff)
     struct stgcat_entry_old *stcp1;
     struct stgcat_entry_old *stcp2;
     int bindiff;
{
  int i;

  if (stcp1 == NULL || stcp2 == NULL) {
    return(-2);
  }

  if (bindiff != 0) {
    return(memcmp(stcp1,stcp2,sizeof(struct stgcat_entry_old)));
  }

  if (       stcp1->blksize               != stcp2->blksize      ||
      strcmp(stcp1->filler,                  stcp2->filler) != 0 ||
             stcp1->charconv              != stcp2->charconv     ||
             stcp1->keep                  != stcp2->keep         ||
             stcp1->lrecl                 != stcp2->lrecl        ||
             stcp1->nread                 != stcp2->nread        ||
             strcmp(stcp1->poolname,                stcp2->poolname) != 0 ||
             strcmp(stcp1->recfm,                   stcp2->recfm) != 0    ||
             stcp1->size                  != stcp2->size         ||
             strcmp(stcp1->ipath,                   stcp2->ipath) != 0    ||
             stcp1->t_or_d                != stcp2->t_or_d       ||
             strcmp(stcp1->group,                   stcp2->group) != 0    ||
             strcmp(stcp1->user,                    stcp2->user) != 0     ||
             stcp1->uid                   != stcp2->uid          ||
             stcp1->gid                   != stcp2->gid          ||
             stcp1->mask                  != stcp2->mask         ||
             stcp1->reqid                 != stcp2->reqid        ||
             stcp1->status                != stcp2->status       ||
             stcp1->actual_size           != stcp2->actual_size  ||
             stcp1->c_time                != stcp2->c_time       ||
             stcp1->a_time                != stcp2->a_time       ||
             stcp1->nbaccesses            != stcp2->nbaccesses) {
    printf("### ... Common part differs\n");
    return(-1);
  }

  if (stcp1->t_or_d != stcp2->t_or_d) {
    printf("### ... t_or_d differs\n");
    return(-1);
  }

  switch (stcp1->t_or_d) {
  case 't':
    if (strcmp(stcp1->u1.t.den,stcp2->u1.t.den) != 0           ||
        strcmp(stcp1->u1.t.dgn,stcp2->u1.t.dgn) != 0           ||
        strcmp(stcp1->u1.t.fid,stcp2->u1.t.fid) != 0           ||
        strcmp(stcp1->u1.t.fseq,stcp2->u1.t.fseq) != 0         ||
        strcmp(stcp1->u1.t.lbl,stcp2->u1.t.lbl) != 0           ||
        stcp1->u1.t.retentd != stcp2->u1.t.retentd             ||
        strcmp(stcp1->u1.t.tapesrvr,stcp2->u1.t.tapesrvr) != 0 ||
        stcp1->u1.t.E_Tflags != stcp2->u1.t.E_Tflags) {
      printf("### ... Tape part differs\n");
      return(-1);
    }
    for (i = 0; i < MAXVSN; i++) {
      if (strcmp(stcp1->u1.t.vid[i],stcp2->u1.t.vid[i]) != 0 ||
          strcmp(stcp1->u1.t.vsn[i],stcp2->u1.t.vsn[i]) != 0) {
        printf("### ... Tape VID or VSN part differs\n");
        return(-1);
      }
    }
    break;
  case 'd':
  case 'a':
    if (strcmp(stcp1->u1.d.xfile,stcp2->u1.d.xfile) != 0 ||
        strcmp(stcp1->u1.d.Xparm,stcp2->u1.d.Xparm) != 0) {
      printf("### ... Disk part differs\n");
      return(-1);
    }
    break;
  case 'm':
    if (strcmp(stcp1->u1.m.xfile,stcp2->u1.m.xfile) != 0) {
      printf("### ... HSM part differs\n");
      return(-1);
    }
    break;
  default:
    printf("### Unknown t_or_d = '%c' type\n",stcp1->t_or_d);
    return(-1);
  }

  /* OKAY */
  return(0);
}

void stcpprint(stcp1,stcp2)
     struct stgcat_entry_old *stcp1;
     struct stgcat_entry_old *stcp2;
{
  int i;

  if (stcp1 == NULL || stcp2 == NULL) {
    return;
  }

  printf("----------------------------------------------------------------\n");
  printf("%10s : %10s ... %10s ... %10s ... %10s\n","What","Catalog1","Catalog2",
         "Status","Bin Status");
  printf("----------------------------------------------------------------\n");
  STCCMP_VAL(blksize);
  STCCMP_STRING(filler);
  STCCMP_CHAR(charconv);
  STCCMP_CHAR(keep);
  STCCMP_VAL(lrecl);
  STCCMP_VAL(nread);
  STCCMP_STRING(poolname);
  STCCMP_STRING(recfm);
  STCCMP_VAL(size);
  STCCMP_STRING(ipath);
  STCCMP_CHAR(t_or_d);
  STCCMP_STRING(group);
  STCCMP_STRING(user);
  STCCMP_VAL(uid);
  STCCMP_VAL(gid);
  STCCMP_VAL(mask);
  STCCMP_VAL(reqid);
  STCCMP_VAL(status);
  STCCMP_VAL(actual_size);
  STCCMP_VAL(c_time);
  STCCMP_VAL(a_time);
  STCCMP_VAL(nbaccesses);
  if (stcp1->t_or_d == stcp2->t_or_d) {
    switch (stcp1->t_or_d) {
    case 't':
      STCCMP_STRING(u1.t.den);
      STCCMP_STRING(u1.t.dgn);
      STCCMP_STRING(u1.t.fid);
      STCCMP_CHAR(u1.t.filstat);
      STCCMP_STRING(u1.t.fseq);
      STCCMP_STRING(u1.t.lbl);
      STCCMP_VAL(u1.t.retentd);
      STCCMP_STRING(u1.t.tapesrvr);
      STCCMP_CHAR(u1.t.E_Tflags);
      for (i = 0; i < MAXVSN; i++) {
        STCCMP_STRING(u1.t.vid[i]);
        STCCMP_STRING(u1.t.vsn[i]);
      }
      break;
    case 'd':
    case 'a':
      STCCMP_STRING(u1.d.xfile);
      STCCMP_STRING(u1.d.Xparm);
      break;
    case 'm':
      STCCMP_STRING(u1.m.xfile);
      break;
    }
  }
  printf("... Total dump of entry in catalog1 and in catalog2:\n");
  shift_cmp_shift_dump2((char *) stcp1,(char *) stcp2, sizeof(struct stgcat_entry_old));
}

int stppcmp(stpp1,stpp2,bindiff)
     struct stgpath_entry_old *stpp1;
     struct stgpath_entry_old *stpp2;
     int bindiff;
{
  if (stpp1 == NULL || stpp2 == NULL) {
    return(-2);
  }

  if (bindiff != 0) {
    return(memcmp(stpp1,stpp2,sizeof(struct stgpath_entry_old)));
  }

  if (stpp1->reqid != stpp2->reqid ||
      strcmp(stpp1->upath,stpp2->upath) != 0) {
    return(-1);
  }

  /* OKAY */
  return(0);
}

void stppprint(stpp1,stpp2)
     struct stgpath_entry_old *stpp1;
     struct stgpath_entry_old *stpp2;
{
  if (stpp1 == NULL || stpp2 == NULL) {
    return;
  }

  printf("----------------------------------------------------------------\n");
  printf("%10s : %10s ... %10s ... %10s ... %10s\n","What","Catalog1","Catalog2",
         "Status","Bin Status");
  printf("----------------------------------------------------------------\n");
  STPCMP_VAL(reqid);
  STPCMP_STRING(upath);
  printf("... Total dump of entry in catalog1 and in catalog2:\n");
  shift_cmp_shift_dump2((char *) stpp1,(char *) stpp2, sizeof(struct stgpath_entry_old));
}

void shift_cmp_shift_dump(buffer, size)
     char *buffer;
     unsigned int size;
{
  int i;
  for (i = 0; i < size; i++) {
    printf("... Byte No %3d | 0x%2x (hex) %3d (dec) %c (char)\n",
           (int) i,
           (unsigned char) buffer[i],
           (unsigned char) buffer[i],
           isprint(buffer[i]) ? buffer[i] : ' ');
  }
}

void shift_cmp_shift_dump2(buffer1, buffer2, size)
     char *buffer1;
     char *buffer2;
     unsigned int size;
{
  int i;
  for (i = 0; i < size; i++) {
    printf("... Byte No %3d | 0x%2x (hex) %3d (dec) %c (char) | 0x%2x (hex) %3d (dec) %c (char)\n",
           (int) i,
           (unsigned char) buffer1[i],
           (unsigned char) buffer1[i],
           isprint(buffer1[i]) ? buffer1[i] : ' ',
           (unsigned char) buffer2[i],
           (unsigned char) buffer2[i],
           isprint(buffer2[i]) ? buffer2[i] : ' ');
  }
}
