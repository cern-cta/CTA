/*
 * $Id: stgconvert.c,v 1.10 2000/01/10 06:58:07 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stgconvert.c,v $ $Revision: 1.10 $ $Date: 2000/01/10 06:58:07 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif

/*
 * Conversion Program between CASTOR/disk stager catalog and CASTOR/Cdb stager database
 */

/* ============== */
/* System headers */
/* ============== */
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>

/* =============================================================== */
/* Local headers for threads : to be included before ANYTHING else */
/* =============================================================== */
#include "osdep.h"
#include "Cdb_api.h"                /* CASTOR Cdb Interface */
#include "Cstage_db.h"              /* Generated STAGE/Cdb header */
#include "stage.h"                  /* CASTOR's STAGE header */
#include "serrno.h"                 /* CASTOR's serrno */
#include "u64subr.h"

/* =============== */
/* Local variables */
/* =============== */
int warns = 1;

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

#define CASTOR_TO_CDB -1
#define CDB_CMP_CASTOR 0
#define CDB_TO_CASTOR  1

#define CDB_USERNAME "Cdb_Stage_User"
#define CDB_PASSWORD "Cdb_Stage_Password"
#define FREQUENCY 1000

#define STCCMP_VAL(member) {                                                    \
  printf("%10s : %10d ... %10d ... %10s ... %10s\n", #member ,                  \
         (int) stcp1->member,(int) stcp2->member,                               \
         stcp1->member == stcp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stcp1->member != stcp2->member || memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog:\n", #member );                     \
    stgconvert_dump((char *) &(stcp1->member),sizeof(stcp1->member));           \
    printf("... Dump of %s entry in Cdb:\n", #member );                         \
    stgconvert_dump((char *) &(stcp2->member),sizeof(stcp2->member));           \
  }                                                                             \
}

#define STCCMP_CHAR(member) {                                                   \
  printf("%10s : %10c ... %10c ... %10s ... %10s\n", #member ,                  \
         stcp1->member != '\0' ? stcp1->member : ' ',stcp2->member != '\0' ? stcp2->member : ' ',                 \
         stcp1->member == stcp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stcp1->member != stcp2->member || memcmp(&(stcp1->member),&(stcp2->member),sizeof(stcp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog:\n", #member );                     \
    stgconvert_dump(&(stcp1->member),sizeof(stcp1->member));                    \
    printf("... Dump of %s entry in Cdb:\n", #member );                         \
    stgconvert_dump(&(stcp2->member),sizeof(stcp2->member));                    \
  }                                                                             \
}

#define STCCMP_STRING(member) {                                                 \
  printf("%10s : %10s ... %10s ... %10s ... %10s\n", #member ,                  \
         stcp1->member,stcp2->member,                                           \
         strcmp(stcp1->member,stcp2->member) == 0 ? "OK" : "NOT OK",            \
         memcmp(stcp1->member,stcp2->member,sizeof(stcp1->member)) == 0 ?       \
         "OK" : "NOT OK");                                                      \
  if (strcmp(stcp1->member,stcp2->member) != 0 || memcmp(stcp1->member,stcp2->member,sizeof(stcp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog:\n", #member );                     \
    stgconvert_dump(stcp1->member,sizeof(stcp1->member));                       \
    printf("... Dump of %s entry in Cdb:\n", #member );                         \
    stgconvert_dump(stcp2->member,sizeof(stcp2->member));                       \
  }                                                                             \
}

#define STPCMP_VAL(member) {                                                    \
  printf("%10s : %10d ... %10d ... %10s ... %10s\n", #member ,                  \
         (int) stpp1->member,(int) stpp2->member,                               \
         stpp1->member == stpp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stpp1->member != stpp2->member || memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog:\n", #member );                     \
    stgconvert_dump((char *) &(stpp1->member),sizeof(stpp1->member));           \
    printf("... Dump of %s entry in Cdb:\n", #member );                         \
    stgconvert_dump((char *) &(stpp2->member),sizeof(stpp2->member));           \
  }                                                                             \
}

#define STPCMP_CHAR(member) {                                                   \
  printf("%10s : %10c ... %10c ... %10s ... %10s\n", #member ,                  \
         stpp1->member != '\0' ? stpp1->member : ' ',stpp2->member != '\0' ? stpp2->member : ' ',                 \
         stpp1->member == stpp2->member ? "OK" : "NOT OK",                      \
         memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) == 0 ? \
         "OK" : "NOT OK");                                                      \
  if (stpp1->member != stpp2->member || memcmp(&(stpp1->member),&(stpp2->member),sizeof(stpp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog:\n", #member );                     \
    stgconvert_dump(&(stpp1->member),sizeof(stpp1->member));                    \
    printf("... Dump of %s entry in Cdb:\n", #member );                         \
    stgconvert_dump(&(stpp2->member),sizeof(stpp2->member));                    \
  }                                                                             \
}

#define STPCMP_STRING(member) {                                                 \
  printf("%10s : %10s ... %10s ... %10s ... %10s\n", #member ,                  \
         stpp1->member,stpp2->member,                                           \
         strcmp(stpp1->member,stpp2->member) == 0 ? "OK" : "NOT OK",            \
         memcmp(stpp1->member,stpp2->member,sizeof(stpp1->member)) == 0 ?       \
         "OK" : "NOT OK");                                                      \
  if (strcmp(stpp1->member,stpp2->member) != 0 || memcmp(stpp1->member,stpp2->member,sizeof(stpp1->member)) != 0) {   \
    printf("... Dump of %s entry in catalog:\n", #member );                     \
    stgconvert_dump(stpp1->member,sizeof(stpp1->member));                       \
    printf("... Dump of %s entry in Cdb:\n", #member );                         \
    stgconvert_dump(stpp2->member,sizeof(stpp2->member));                       \
  }                                                                             \
}

/* =================== */
/* Internal prototypes */
/* =================== */
void stgconvert_usage _PROTO(());
int stcpcmp _PROTO((struct stgcat_entry *, struct stgcat_entry *, int));
int stppcmp _PROTO((struct stgpath_entry *, struct stgpath_entry *, int));
void stcpprint _PROTO((struct stgcat_entry *, struct stgcat_entry *));
void stppprint _PROTO((struct stgpath_entry *, struct stgpath_entry *));
int stgdb_stcpcmp _PROTO((CONST void *, CONST void *));
int stgdb_stppcmp _PROTO((CONST void *, CONST void *));
void stgconvert_dump _PROTO((char *, unsigned int));
void stgconvert_dump2 _PROTO((char *, char *, unsigned int));

int main(argc,argv)
     int argc;
     char **argv;
{
  /* For options */
  extern char *optarg;
  extern int optind, opterr, optopt;
  int errflg = 0;
  int c;
  int convert_direction = 0;         /* 1 : CDB -> CASTOR, -1 : CASTOR -> CDB */
  int help = 0;                      /* 1 : help wanted */
  char *stgcat = NULL;               /* CASTOR's stager catalog path */
  int stgcat_fd = -1;
  char *stgpath = NULL;              /* CASTOR's stager link catalog path */
  int stgpath_fd = -1;
  struct stat stgcat_statbuff, stgpath_statbuff;
  char *Cdb_username = NULL;
  char *Cdb_password = NULL;
  int rc;
  Cdb_sess_t Cdb_session;
  int Cdb_session_opened = 0;
  Cdb_db_t Cdb_db;
  int Cdb_db_opened = 0;
  int no_stgcat = 0;
  int no_stgpath = 0;
  int bindiff = 0;
  int maxstcp = -1;
  int maxstpp = -1;
  char *error = NULL;
  struct stgcat_entry *stcp = NULL;  /* stage catalog pointer */
  struct stgcat_entry *stcs = NULL;  /* start of stage catalog pointer */
  struct stgcat_entry *stce = NULL;  /* end of stage catalog pointer */
  struct stgpath_entry *stpp = NULL; /* stage path catalog pointer */
  struct stgpath_entry *stps = NULL; /* start of stage path catalog pointer */
  struct stgpath_entry *stpe = NULL; /* end of stage path catalog pointer */
  struct stgcat_tape tape;
  struct stgcat_disk disk;
  struct stgcat_hsm hsm;
  struct stgcat_link link;
  struct stgcat_alloc alloc;
  Cdb_off_t Cdb_offset;
  char t_or_d = '\0';
  char tmpbuf[21];
  int frequency = FREQUENCY;

  while ((c = getopt(argc,argv,"bc:Cg:hl:Ln:u:p:qt:v")) != EOF) {
    switch (c) {
    case 'b':
      bindiff = 1;
      break;
    case 'c':
      maxstcp = atoi(optarg);
      break;
    case 'g':
      convert_direction = atoi(optarg);
      break;
    case 'h':
      help = 1;
      break;
    case 'C':
      no_stgcat = 1;
      break;
    case 'l':
      maxstpp = atoi(optarg);
      break;
    case 'L':
      no_stgpath = 1;
      break;
    case 'n':
      frequency = atoi(optarg);
      break;
    case 'q':
      warns = 0;
      break;
    case 'u':
      Cdb_username = optarg;
      break;
    case 'p':
      Cdb_password = optarg;
      break;
    case 't':
      if (strcmp(optarg,"t") != 0 &&
          strcmp(optarg,"d") != 0 &&
          strcmp(optarg,"m") != 0 &&
          strcmp(optarg,"a") != 0) {
        printf("### Only \"t\", \"d\", \"m\" or \"a\" is allowed to -t option value.\n");
        return(EXIT_FAILURE);
      }
      t_or_d = optarg[0];
      break;
    case 'v':
      printf("%s\n",sccsid);
      return(EXIT_SUCCESS);
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
      stgconvert_usage();
      return(EXIT_FAILURE);
  }

  if (help != 0) {
    stgconvert_usage();
    return(EXIT_SUCCESS);
  }

  if (convert_direction != CDB_TO_CASTOR && 
      convert_direction != CASTOR_TO_CDB &&
      convert_direction != CDB_CMP_CASTOR) {
    printf("?? -g option is REQUIRED and HAS to be either %d (CDB->CASTOR), %d (CASTOR->CDB) or %d (CDB cmp CASTOR), nothing else.\n",
            CDB_TO_CASTOR,CASTOR_TO_CDB,CDB_CMP_CASTOR);
    stgconvert_usage();
    return(EXIT_FAILURE);
  }

  if (optind >= argc || optind > (argc - 2)) {
    printf("?? Exactly two parameters are requested\n");
    stgconvert_usage();
    return(EXIT_FAILURE);
  }

  stgcat = argv[optind];
  stgpath = argv[optind+1];

  if (convert_direction == CASTOR_TO_CDB) {
    if (no_stgcat == 0) {
      if ((stgcat_fd = open(stgcat, FILE_OFLAG | O_RDWR | O_CREAT, FILE_MODE)) < 0) {
        printf("### open of %s error, %s\n",stgcat,strerror(errno));
        return(EXIT_FAILURE);
      }
    }
    if (no_stgpath == 0) {
      if ((stgpath_fd = open(stgpath, FILE_OFLAG | O_RDWR | O_CREAT, FILE_MODE)) < 0) {
        printf("### open of %s error, %s\n",stgpath,strerror(errno));
        close(stgcat_fd);
        return(EXIT_FAILURE);
      }
    }
  } else {
    if (no_stgcat == 0) {
      if ((stgcat_fd = open(stgcat, FILE_OFLAG, FILE_MODE)) < 0) {
        printf("### open of %s error, %s\n",stgcat,strerror(errno));
        return(EXIT_FAILURE);
      }
    }
    if (no_stgpath == 0) {
      if ((stgpath_fd = open(stgpath, FILE_OFLAG, FILE_MODE)) < 0) {
        printf("### open of %s error, %s\n",stgpath,strerror(errno));
        close(stgcat_fd);
        return(EXIT_FAILURE);
      }
    }
  }

  /* == From now on we will goto stgconvert_return for any exit == */

  if (no_stgcat == 0) {
    if (fstat(stgcat_fd, &stgcat_statbuff) < 0) {
      printf("### fstat on %s error, %s\n"
             ,stgcat
             ,strerror(errno));
      rc = EXIT_FAILURE;
      goto stgconvert_return;
    }
  }
  if (no_stgpath == 0) {
    if (fstat(stgpath_fd, &stgpath_statbuff) < 0) {
      printf("### fstat on %s error, %s\n"
             ,stgpath
             ,strerror(errno));
      rc = EXIT_FAILURE;
      goto stgconvert_return;
    }
  }

  /* If the user said CASTOR -> CDB conversion and one of those files is of length > 0   */
  /* he will overwrite existing non-zero length files. We then check if that's really what */
  /* he wants to do.                                                                       */
  if (convert_direction == CASTOR_TO_CDB) {
    if (warns != 0) {
      if ((no_stgcat == 0  && stgcat_statbuff.st_size > 0) || 
          (no_stgpath == 0 && stgpath_statbuff.st_size > 0)) {
        int answer;
        
        printf("### Warning : You are going to truncate a disk catalog that is of non-zero length\n");
        if (no_stgcat == 0) {
          printf("### Current %s length  : %s\n",stgcat,
                 u64tostr((u_signed64) stgcat_statbuff.st_size, tmpbuf, 0));
        }
        if (no_stgpath == 0) {
          printf("### Current %s length : %s\n",stgpath,
                 u64tostr((u_signed64) stgpath_statbuff.st_size, tmpbuf, 0));
        }
        printf("### Do you really want to proceed (y/n) ? ");
        
        answer = getchar();
        if (answer != 'y' && answer != 'Y') {
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
        /* User said yes. This means that we are going to truncate the files if they already exist */
        /* and have non-zero length.                                                               */
      }
    }
  }

  /* We open a connection to Cdb */
  if (Cdb_username == NULL) {
    Cdb_username = CDB_USERNAME;
  }
  if (Cdb_password == NULL) {
    Cdb_password = CDB_PASSWORD;
  }
  if (Cdb_login(Cdb_username,Cdb_password,&Cdb_session) != 0) {
    printf("### Cdb_login(\"%s\",\"%s\",&Cdb_session) error, %s - %s\n"
           ,Cdb_username
           ,Cdb_password
           ,sstrerror(serrno)
           ,strerror(errno));
    if (Cdb_error(&Cdb_session,&error) == 0) {
      printf("--> more info:\n%s",error);
    }
    rc = EXIT_FAILURE;
    goto stgconvert_return;
    
  }

  Cdb_session_opened = -1;

  /* We open the database */
  if (Cdb_open(&Cdb_session,"stage",&Cdb_stage_interface,&Cdb_db) != 0) {
    printf("### Cdb_open(&Cdb_session,\"stage\",&Cdb_stage_interface,&Cdb_db) error, %s\n"
            ,sstrerror(serrno));
    if (Cdb_error(&Cdb_session,&error) == 0) {
      printf("--> more info:\n%s",error);
    }
    printf("### PLEASE SUBMIT THE STAGER DATABASE WITH THE TOOL Cdb_create IF NOT ALREADY DONE.\n");
    rc = EXIT_FAILURE;
    goto stgconvert_return;
  }

  Cdb_db_opened = -1;

  if (convert_direction == CDB_TO_CASTOR) {
    /* ========================== */
    /* CDB -> CASTOR conversion */
    /* ========================== */
    int i = 0;
    int nstcp = 0;
    int nstpp = 0;

    if (no_stgcat == 0) {
      if (stgcat_statbuff.st_size > 0) {
        if ((stcs = malloc(stgcat_statbuff.st_size)) == NULL) {
          printf("### malloc error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
        printf("... Loading %s\n",stgcat);
        if (read(stgcat_fd,stcs,stgcat_statbuff.st_size) != stgcat_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stce = stcs;
      stce += (stgcat_statbuff.st_size/sizeof(struct stgcat_entry));
      /* We count the number of entries in the catalog */
      for (stcp = stcs; stcp < stce; stcp++) {
        if (stcp->reqid == 0) {
          break;
        }
        if (t_or_d != '\0' && t_or_d != stcp->t_or_d) {
          continue;
        }
        ++nstcp;
      }
    }
    if (no_stgpath == 0) {
      if (stgpath_statbuff.st_size > 0) {
        if ((stps = malloc(stgpath_statbuff.st_size)) == NULL) {
          printf("### malloc error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
        printf("... Loading %s\n",stgpath);
        if (read(stgpath_fd,stps,stgpath_statbuff.st_size) != stgpath_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stpe = stps;
      stpe += (stgpath_statbuff.st_size/sizeof(struct stgpath_entry));
      for (stpp = stps; stpp < stpe; stpp++) {
        struct stgpath_entry *stpp2;
        struct stgpath_entry *stppok;
        if (stpp->reqid == 0) {
          break;
        }
        ++nstpp;
      }
      
    }


    if (no_stgcat == 0) {
      printf("\n*** CONVERTING stgcat CATALOG ***\n\n");
      
      /* We loop on the stgcat catalog */
      /* ------------------------------------- */
      i = 0;
      for (stcp = stcs; stcp < stce; stcp++) {
        if (stcp->reqid == 0) {
          printf("==> %6d over a total of %6d stgcat entries have been transfered\n",
                 i,nstcp);
          break;
        }
        ++i;
        if (maxstcp >= 0 && i > maxstcp) {
          break;
        }
        if (stcp2Cdb(stcp,&tape,&disk,&hsm,&alloc) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
        }

        if (t_or_d != '\0' && t_or_d != stcp->t_or_d) {
          continue;
        }

        /* We insert this record vs. its t_or_d type */
        switch (stcp->t_or_d) {
        case 't':
          if (Cdb_insert(&Cdb_db,"stgcat_tape",NULL,&tape,&Cdb_offset) != 0) {
            printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_tape\" (%s)\n"
                   ,tape.reqid
                   ,sstrerror(serrno));
          } else {
            if (i == 1 || i % frequency == 0) {
              printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_tape\" [c_time=0x%lx,VID[0]=%s,FSEQ=%s] at offset %s\n",
                     i,nstcp,tape.reqid,(unsigned long) tape.c_time,tape.vid[0],tape.fseq,
                     u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
            }
          }
          break;
        case 'd':
          if (Cdb_insert(&Cdb_db,"stgcat_disk",NULL,&disk,&Cdb_offset) != 0) {
            printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_disk\" (%s)\n"
                   ,disk.reqid
                   ,sstrerror(serrno));
          } else {
            if (i == 1 || i % frequency == 0) {
              printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_disk\" [c_time=0x%lx,xfile=%s] at offset %s\n",
                     i,nstcp,disk.reqid,(unsigned long) disk.c_time,disk.xfile,
                     u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
            }
          }
          break;
        case 'm':
          if (Cdb_insert(&Cdb_db,"stgcat_hsm",NULL,&hsm,&Cdb_offset) != 0) {
            printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_hsm\" (%s)\n"
                   ,hsm.reqid
                   ,sstrerror(serrno));
          } else {
            if (i == 1 || i % frequency == 0) {
              printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_hsm\" [c_time=0x%lx,xfile=%s] at offset %s\n",
                     i,nstcp,hsm.reqid,(unsigned long) hsm.c_time,hsm.xfile,
                     u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
            }
          }
          break;
        case 'a':
          if (Cdb_insert(&Cdb_db,"stgcat_alloc",NULL,&alloc,&Cdb_offset) != 0) {
            printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_alloc\" (%s)\n"
                   ,alloc.reqid
                   ,sstrerror(serrno));
          } else {
            if (i == 1 || i % frequency == 0) {
              printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_alloc\" [c_time=0x%lx,xfile=%s] at offset %s\n",
                     i,nstcp,alloc.reqid,(unsigned long) alloc.c_time,alloc.xfile,
                     u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
            }
          }
          break;
        default:
          printf("### stcp->t_or_d == '%c' unrecognized. Please update this program.\n",stcp->t_or_d);
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }

      }
    }

    if (no_stgpath == 0) {
      printf("\n*** CONVERTING stgpath CATALOG ***\n\n");

    /* We loop on the stgpath catalog */
    /* -------------------------------------- */
      i = 0;
      for (stpp = stps; stpp < stpe; stpp++) {
        if (stpp->reqid == 0) {
          printf("==> %6d over a total of %6d stgcat entries have been transfered\n",
                 i,nstpp);
          break;
        }
        ++i;
        if (maxstpp >= 0 && i > maxstpp) {
          break;
        }
        if (stpp2Cdb(stpp,&link) != 0) {
          printf("### stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d\n",stpp->reqid);
        }

        /* stgpath can have multiple entries - We silently delete previous one if it */
        /* exists.                                                                   */
        if (Cdb_keyfind(&Cdb_db,"stgcat_link","stgcat_link_per_reqid","w",&link,&Cdb_offset) == 0) {
          if (warns != 0) {
            printf("### Warning (harmless) : stpp entry with reqid %6d yet exists at offset %s.\n",
                   link.reqid,
                   u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
          }
          if (Cdb_delete(&Cdb_db,"stgcat_link",&Cdb_offset) != 0) {
            printf("### Cannot delete previous stgpath occurence for reqid=%d\n",link.reqid);
            if (Cdb_unlock(&Cdb_db,"stgcat_link",&Cdb_offset) != 0) {
              printf("### Cannot remove lock in Cdb at offset %s\n",
                     u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
            }
            continue;
          }
          if (Cdb_unlock(&Cdb_db,"stgcat_link",&Cdb_offset) != 0) {
            printf("### Cannot remove lock in Cdb at offset %s\n",
                   u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
          }
        }

        /* We insert this record */
        if (Cdb_insert(&Cdb_db,"stgcat_link",NULL,&link,&Cdb_offset) != 0) {
          printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_link\" (%s)\n"
                 ,stpp->reqid
                 ,sstrerror(serrno));
        } else {
          if (i == 1 || i % frequency == 0) {
            printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_link\" [upath=%s] at offset %s\n",
                   i,nstpp,link.reqid,link.upath,
                   u64tostr((u_signed64) Cdb_offset, tmpbuf, 0));
          }
        }
      }
    }

  } else if (convert_direction == CASTOR_TO_CDB) {

    /* ========================== */
    /* CASTOR -> CDB conversion */
    /* ========================== */
    
    int i = 0;

    if (no_stgcat == 0) {
      /* We truncate the files */
      if (ftruncate(stgcat_fd, (size_t) 0) != 0) {
        printf("### ftruncate on %s error, %s\n"
               ,stgcat
               ,strerror(errno));
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
    }
    if (no_stgpath == 0) {
      if (ftruncate(stgpath_fd, (size_t) 0) != 0) {
        printf("### ftruncate on %s error, %s\n"
               ,stgpath
               ,strerror(errno));
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
    }

    if (no_stgcat == 0) {
      if (t_or_d != '\0' && t_or_d != 't') {
        goto no_tape_dump;
      }

      printf("\n*** DUMPING stgcat_tape TABLE ***\n\n");

    /* We ask for a dump of tape table from Cdb */
    /* ---------------------------------------- */
      if (Cdb_dump_start(&Cdb_db,"stgcat_tape") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_tape\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_tape",&Cdb_offset,&tape) == 0) {
        struct stgcat_entry thisstcp;

        if (Cdb2stcp(&thisstcp,&tape,NULL,NULL,NULL) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",tape.reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          ++i;
          if (i == 1 || i % frequency == 0) {
            printf("--> (%6d) reqid = %d OK\n",i,thisstcp.reqid);
          }
        }

      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_tape") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_tape\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    no_tape_dump:

      if (t_or_d != '\0' && t_or_d != 'd') {
        goto no_disk_dump;
      }

      /* We ask for a dump of disk table from Cdb */
      /* ---------------------------------------- */

      printf("\n*** DUMPING stgcat_disk TABLE ***\n\n");

      if (Cdb_dump_start(&Cdb_db,"stgcat_disk") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_disk\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_disk",&Cdb_offset,&disk) == 0) {
        struct stgcat_entry thisstcp;

        if (Cdb2stcp(&thisstcp,NULL,&disk,NULL,NULL) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",disk.reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          ++i;
          if (i == 1 || i % frequency == 0) {
            printf("--> (%6d) reqid = %d OK\n",i,thisstcp.reqid);
          }
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_disk") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_disk\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    no_disk_dump:

      if (t_or_d != '\0' && t_or_d != 'm') {
        goto no_migrated_dump;
      }

      /* We ask for a dump of hsm table from Cdb */
      /* --------------------------------------- */

      printf("\n*** DUMPING stgcat_hsm TABLE ***\n\n");

      if (Cdb_dump_start(&Cdb_db,"stgcat_hsm") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_hsm\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_hsm",&Cdb_offset,&hsm) == 0) {
        struct stgcat_entry thisstcp;

        if (Cdb2stcp(&thisstcp,NULL,NULL,&hsm,NULL) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",hsm.reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          ++i;
          if (i == 1 || i % frequency == 0) {
            printf("--> (%6d) reqid = %d OK\n",i,thisstcp.reqid);
          }
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_hsm") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_hsm\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    no_migrated_dump:

      if (t_or_d != '\0' && t_or_d != 'a') {
        goto no_alloced_dump;
      }

      /* We ask for a dump of alloc table from Cdb */
      /* ----------------------------------------- */

      printf("\n*** DUMPING stgcat_alloc TABLE ***\n\n");

      if (Cdb_dump_start(&Cdb_db,"stgcat_alloc") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_alloc\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_alloc",&Cdb_offset,&alloc) == 0) {
        struct stgcat_entry thisstcp;

        if (Cdb2stcp(&thisstcp,NULL,NULL,NULL,&alloc) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",alloc.reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          ++i;
          if (i == 1 || i % frequency == 0) {
            printf("--> (%6d) reqid = %d OK\n",i,thisstcp.reqid);
          }
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_alloc") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_alloc\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

      /* We reload all the entries into memory */
      {
        struct stat statbuff;

        if (fstat(stgcat_fd,&statbuff) < 0) {
          printf("### fstat error on \"%s\" (%s)\n",stgcat,strerror(errno));
          printf("### No stgcat sorting possible\n");
        } else {
          if (statbuff.st_size > 0) {
            struct stgcat_entry *stcpall;

            if ((stcpall = (struct stgcat_entry *) malloc(statbuff.st_size)) == NULL) {
              printf("### malloc error (%s)\n",strerror(errno));
              printf("### No stgcat sorting possible\n");
            } else {
              if (lseek(stgcat_fd,0,SEEK_SET) < 0) {
                printf("### lseek error on \"%s\" (%s)\n",stgcat,strerror(errno));
                printf("### No stgcat sorting possible\n");
              } else {
                printf("... Re-loading %s\n",stgcat);
                if (read(stgcat_fd,stcpall,statbuff.st_size) != statbuff.st_size) {
                  printf("### read error on \"%s\" (%s)\n",stgcat,strerror(errno));
                  printf("### No stgcat sorting possible\n");
                } else {
                  printf("... Sorting %s content\n",stgcat);
                  qsort(stcpall,statbuff.st_size/sizeof(struct stgcat_entry),
                        sizeof(struct stgcat_entry), &stgdb_stcpcmp);
                  if (lseek(stgcat_fd,0,SEEK_SET) < 0) {
                    printf("### lseek error on \"%s\" (%s)\n",stgcat,strerror(errno));
                    printf("### No stgcat sorted output possible\n");
                  } else {
                    printf("... Saving %s\n",stgcat);
                    if (write(stgcat_fd,stcpall,statbuff.st_size) != statbuff.st_size) {
                      printf("### write error on \"%s\" (%s)\n",stgcat,strerror(errno));
                      printf("### Sorted stgcat output probably corrupted\n");
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  no_alloced_dump:

    if (no_stgpath == 0) {
      /* We ask for a dump of stgpath master table from Cdb */
      /* -------------------------------------------------- */
      printf("\n*** DUMPING stgcat_link TABLE ***\n\n");
      
      i = 0;
      
      if (Cdb_dump_start(&Cdb_db,"stgcat_link") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_link\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
      
      while (Cdb_dump(&Cdb_db,"stgcat_link",&Cdb_offset,&link) == 0) {
        struct stgpath_entry thisstpp;
        int convert_status = 0;
        
        if (Cdb2stpp(&thisstpp,&link) != 0) {
          printf("### Cannot convert entry with reqid = %d from Cdb's table \"stgcat_link\"\n"
                 ,link.reqid);
          convert_status = -1;
        }
        
        if (convert_status == 0) {
          if (write(stgpath_fd,&thisstpp,sizeof(struct stgpath_entry)) != sizeof(struct stgpath_entry)) {
            printf("### write() error on %s (%s)\n",stgpath,strerror(errno));
          } else {
            ++i;
            if (i == 1 || i % frequency == 0) {
              printf("--> (%6d) reqid = %d converted back\n",i,thisstpp.reqid);
            }
          }
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_link") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_link\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }
      /* We reload all the entries into memory */
      {
        struct stat statbuff;
        
        if (fstat(stgpath_fd,&statbuff) < 0) {
          printf("### fstat error on \"%s\" (%s)\n",stgpath,strerror(errno));
          printf("### No stgpath sorting possible\n");
        } else {
          if (statbuff.st_size > 0) {
            struct stgpath_entry *stppall;
            
            if ((stppall = (struct stgpath_entry *) malloc(statbuff.st_size)) == NULL) {
              printf("### malloc error (%s)\n",strerror(errno));
              printf("### No stgpath sorting possible\n");
            } else {
              if (lseek(stgpath_fd,0,SEEK_SET) < 0) {
                printf("### lseek error on \"%s\" (%s)\n",stgpath,strerror(errno));
                printf("### No stgpath sorting possible\n");
              } else {
                printf("... Re-loading %s\n",stgpath);
                if (read(stgpath_fd,stppall,statbuff.st_size) != statbuff.st_size) {
                  printf("### read error on \"%s\" (%s)\n",stgpath,strerror(errno));
                  printf("### No stgpath sorting possible\n");
                } else {
                  printf("... Sorting %s content\n",stgpath);
                  qsort(stppall,statbuff.st_size/sizeof(struct stgpath_entry),
                        sizeof(struct stgpath_entry), &stgdb_stppcmp);
                  if (lseek(stgpath_fd,0,SEEK_SET) < 0) {
                    printf("### lseek error on \"%s\" (%s)\n",stgpath,strerror(errno));
                    printf("### No stgpath sorted output possible\n");
                  } else {
                    printf("... Saving %s\n",stgpath);
                    if (write(stgpath_fd,stppall,statbuff.st_size) != statbuff.st_size) {
                      printf("### write error on \"%s\" (%s)\n",stgpath,strerror(errno));
                      printf("### Sorted stgpath output probably corrupted\n");
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  } else if (convert_direction == CDB_CMP_CASTOR) {
    /* =========================== */
    /* CDB cmp CASTOR conversion */
    /* =========================== */

    int i = 0;
    int nstcp = 0;
    int nstpp = 0;

    int global_stgcat_cmp_status = 0;

    if (no_stgcat == 0) {
      if (stgcat_statbuff.st_size > 0) {
        if ((stcs = malloc(stgcat_statbuff.st_size)) == NULL) {
          printf("### malloc error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
        printf("... Loading %s\n",stgcat);
        if (read(stgcat_fd,stcs,stgcat_statbuff.st_size) != stgcat_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stce = stcs;
      stce += (stgcat_statbuff.st_size/sizeof(struct stgcat_entry));
      /* We count the number of entries in the catalog */
      for (stcp = stcs; stcp < stce; stcp++) {
        if (stcp->reqid == 0) {
          break;
        }
        if (t_or_d != '\0' && t_or_d != stcp->t_or_d) {
          continue;
        }
        ++nstcp;
      }
    }
    if (no_stgpath == 0) {
      if (stgpath_statbuff.st_size > 0) {
        if ((stps = malloc(stgpath_statbuff.st_size)) == NULL) {
          printf("### malloc error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
        printf("... Loading %s\n",stgpath);
        if (read(stgpath_fd,stps,stgpath_statbuff.st_size) != stgpath_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stpe = stps;
      stpe += (stgpath_statbuff.st_size/sizeof(struct stgpath_entry));
      for (stpp = stps; stpp < stpe; stpp++) {
        if (stpp->reqid == 0) {
          break;
        }
        ++nstpp;
      }
    }

    if (no_stgcat == 0) {
      printf("\n*** COMPARING stgcat CATALOG for CASTOR/disk and CASTOR/CDB ***\n\n");

      if (t_or_d != '\0' && t_or_d != 't') {
        goto no_tape_cmp;
      }

      printf("\n*** DUMPING stgcat_tape TABLE ***\n\n");

    /* We ask for a dump of tape table from Cdb */
    /* ---------------------------------------- */
      if (Cdb_dump_start(&Cdb_db,"stgcat_tape") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_tape\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_tape",&Cdb_offset,&tape) == 0) {
        struct stgcat_entry thisstcp;
        int cmp_status = -2;

        if (Cdb2stcp(&thisstcp,&tape,NULL,NULL,NULL) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",tape.reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp,bindiff);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
            break;
          }
        }

        ++i;
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          if (i == 1 || i % frequency == 0) {
            printf("... (%6d/%6d) reqid = %d Comparison OK\n",i,nstcp,thisstcp.reqid);
          }
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",i,nstcp,thisstcp.reqid);
          stcp->reqid *= -1;
          stcpprint(stcp,&thisstcp);
          stcp->reqid *= -1;
          global_stgcat_cmp_status = -1;
        }

      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_tape") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_tape\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    no_tape_cmp:

      if (t_or_d != '\0' && t_or_d != 'd') {
        goto no_disk_cmp;
      }

      printf("\n*** DUMPING stgcat_disk TABLE ***\n\n");

    /* We ask for a dump of disk table from Cdb */
    /* ---------------------------------------- */
      if (Cdb_dump_start(&Cdb_db,"stgcat_disk") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_disk\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_disk",&Cdb_offset,&disk) == 0) {
        struct stgcat_entry thisstcp;
        int cmp_status = -2;
        if (Cdb2stcp(&thisstcp,NULL,&disk,NULL,NULL) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",disk.reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp,bindiff);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
            break;
          }
        }
        ++i;
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          if (i == 1 || i % frequency == 0) {
            printf("... (%6d/%6d) reqid = %d Comparison OK\n",i,nstcp,thisstcp.reqid);
          }
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",i,nstcp,thisstcp.reqid);
          stcp->reqid *= -1;
          stcpprint(stcp,&thisstcp);
          stcp->reqid *= -1;
          global_stgcat_cmp_status = -1;
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_disk") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_disk\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    no_disk_cmp:

      if (t_or_d != '\0' && t_or_d != 'm') {
        goto no_migrated_cmp;
      }

      printf("\n*** DUMPING stgcat_hsm TABLE ***\n\n");

    /* We ask for a dump of hsm table from Cdb */
    /* ---------------------------------------- */
      if (Cdb_dump_start(&Cdb_db,"stgcat_hsm") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_hsm\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_hsm",&Cdb_offset,&hsm) == 0) {
        struct stgcat_entry thisstcp;
        int cmp_status = -2;
        if (Cdb2stcp(&thisstcp,NULL,NULL,&hsm,NULL) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",hsm.reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp,bindiff);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
            break;
          }
        }
        ++i;
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          if (i == 1 || i % frequency == 0) {
            printf("... (%6d/%6d) reqid = %d Comparison OK\n",i,nstcp,thisstcp.reqid);
          }
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",i,nstcp,thisstcp.reqid);
          stcp->reqid *= -1;
          stcpprint(stcp,&thisstcp);
          stcp->reqid *= -1;
          global_stgcat_cmp_status = -1;
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_hsm") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_hsm\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    no_migrated_cmp:

      if (t_or_d != '\0' && t_or_d != 'a') {
        goto no_alloced_cmp;
      }

      printf("\n*** DUMPING stgcat_alloc TABLE ***\n\n");

    /* We ask for a dump of alloc table from Cdb */
    /* ---------------------------------------- */
      if (Cdb_dump_start(&Cdb_db,"stgcat_alloc") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_alloc\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_alloc",&Cdb_offset,&alloc) == 0) {
        struct stgcat_entry thisstcp;
        int cmp_status = -2;
        if (Cdb2stcp(&thisstcp,NULL,NULL,NULL,&alloc) != 0) {
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",alloc.reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp,bindiff);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
            break;
          }
        }
        ++i;
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          if (i == 1 || i % frequency == 0) {
            printf("... (%6d/%6d) reqid = %d Comparison OK\n",i,nstcp,thisstcp.reqid);
          }
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",i,nstcp,thisstcp.reqid);
          stcp->reqid *= -1;
          stcpprint(stcp,&thisstcp);
          stcp->reqid *= -1;
          global_stgcat_cmp_status = -1;
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_alloc") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_alloc\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    }

    no_alloced_cmp:
    if (no_stgpath == 0) {
      printf("\n*** DUMPING stgcat_link TABLE ***\n\n");

      i = 0;

      /* We ask for a dump of link table from Cdb */
      /* ---------------------------------------- */
      if (Cdb_dump_start(&Cdb_db,"stgcat_link") != 0) {
        printf("### Cdb_dump_start error on table \"stgcat_link\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      while (Cdb_dump(&Cdb_db,"stgcat_link",&Cdb_offset,&link) == 0) {
        struct stgpath_entry thisstpp;
        int cmp_status = -2;

        if (Cdb2stpp(&thisstpp,&link) != 0) {
          printf("### Cdb2stpp (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d\n",link.reqid);
          continue;
        }

        for (stpp = stps; stpp < stpe; stpp++) {
          if (stpp->reqid == 0) {
            break;
          }
          /* It will compare all the version containing the same reqid, up to the last one ! */
          /* eg up to the correct one.                                                       */
          if (stpp->reqid == thisstpp.reqid) {
            cmp_status = stppcmp(stpp,&thisstpp,bindiff);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stpp->reqid *= -1;
            /* break; */
          }
        }
        ++i;
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",i,nstpp,thisstpp.reqid,stgpath);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          if (i == 1 || i % frequency == 0) {
            printf("... (%6d/%6d) reqid = %d Comparison OK\n",i,nstpp,thisstpp.reqid);
          }
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",i,nstpp,thisstpp.reqid);
          stpp->reqid *= -1;
          stppprint(stpp,&thisstpp);
          stpp->reqid *= -1;
          global_stgcat_cmp_status = -1;
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_link") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_link\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

      printf("... Comparison finished, global status : %d (%s)\n",
             global_stgcat_cmp_status,
             global_stgcat_cmp_status == 0 ? "OK" : "NOT OK");
      
      /* If i != nstpp, we verify that i corresponds exactly to the number of unique entries */
      if (i != nstpp && i > 0) {

        printf("[### NOTA ### stgpath catalog in Cdb contains unique versions vs. reqid, while yours can]\n");
        printf("[             contain multiple ones vs. reqid. That is, only the last reqid is compared.]\n");
        printf("[             This behaviour is normal, and if the global status below is OK, then you  ]\n");
        printf("[             are done well with stpath conversion.                                     ]\n");
        printf("... %d != %d (total entries in %s). Checking the number of unique entries vs. reqid in the disk catalog version\n",
               i,nstpp,stgpath);
        
        for (stpp = stps; stpp < stpe; stpp++) {
          if (stpp->reqid == 0) {
            break;
          }
          if (stpp->reqid > 0) {
            printf("### %s member with reqid = %d is not seen in the dump back\n",stgpath,stpp->reqid);
            global_stgcat_cmp_status = -1;
          }
        }
      }
    }

    printf("... Comparison finished, global status : %d (%s)\n",
           global_stgcat_cmp_status,
           global_stgcat_cmp_status == 0 ? "OK" : "NOT OK");
  } else {
    printf("### Unknown -g option value (%d)\n",convert_direction);
  }

 stgconvert_return:
  if (stcs != NULL) {
    free(stcs);
  }
  if (stps != NULL) {
    free(stps);
  }
  if (Cdb_db_opened != 0) {
    Cdb_close(&Cdb_db);
  }
  if (Cdb_session_opened != 0) {
    Cdb_logout(&Cdb_session);
  }
  close(stgcat_fd);
  close(stgpath_fd);
  return(rc);
}

void stgconvert_usage() {
  printf(
         "\n"
         "Usage : stgconvert -g <number> [options] stgcat_path stgpath_path\n"
         "\n"
         "  This program will convert from and to the CASTOR/disk stager catalog\n"
         "and the CASTOR interfaced to Cdb database.\n"
         "\n"
         "Options are:\n"
         "  -h                Print this help and exit\n"
         "  -b                Do a binary diff when doing CASTOR/Cdb cmp CASTOR/disk\n"
         "  -C                Do nothing about stgcat\n"
         "  -c <number>       maximum number to convert from CASTOR/Cdb to/from CASTOR/disk for stgcat\n"
         "  -g <number>       Convert direction, where\n"
         "                    -g %2d means: CDB -> CASTOR\n"
         "                    -g %2d means: CDB cmp CASTOR\n"
         "                    -g %2d means: CASTOR -> CDB\n"
         "  -l <number>       maximum number to convert from CDB to CASTOR for stgpath\n"
         "  -L                Do nothing about stgpath\n"
         "  -n                Output frequency. Default is every %d entries\n"
         "  -u                Cdb username. Defaults to \"%s\"\n"
         "  -p                Cdb password. Defaults to \"%s\"\n"
         "  -q                Do not print warnings\n"
         "  -t <type>         Restrict stgcat catalog to data of type:\n"
         "                    \"t\" (tape)\n"
         "                    \"d\" (disk)\n"
         "                    \"m\" (migrated)\n"
         "                    \"a\" (alloced)\n"
         "  -v                Print version and exit\n"
         "\n"
         "Note : It can very well be that Cdb is running with a zero length or non-existing\n"
         "       password file. In this case you can ignore -u and -p options.\n"
         "       If this is not the case and if you don't know the Cdb username and/or password\n"
         "       either contact somebody who knows, either delete the Cdb password file, which\n"
         "       should be located at /usr/spool/db/Cdb.pwd\n"
         "       If you specify CDB -> CASTOR conversion you are suggested to reset the\n"
         "       \"stage\" database inside Cdb. This is achieved by removing the directory\n"
         "       /usr/spool/db/stage/.\n"
         "\n"
         " Example of CDB->CASTOR convertion:\n"
         "  stgconvert -g %2d /usr/spool/stage/stgcat /usr/spool/stage/stgpath\n"
         "\n"
         " Example of CDB/CASTOR comparison:\n"
         "  stgconvert -g %2d /usr/spool/stage/stgcat /usr/spool/stage/stgpath\n"
         "\n"
         " Example of CASTOR->CDB convertion:\n"
         "  stgconvert -g %2d /usr/spool/stage/stgcat.new /usr/spool/stage/stgpath.new\n"
         "\n"
         "Comments to castor-support@listbox.cern.ch\n"
         "\n",
         CDB_TO_CASTOR,CDB_CMP_CASTOR,CASTOR_TO_CDB,
         FREQUENCY,
         CDB_USERNAME,CDB_PASSWORD,
         CDB_TO_CASTOR,CDB_CMP_CASTOR,CASTOR_TO_CDB
         );
}

int stcpcmp(stcp1,stcp2,bindiff)
     struct stgcat_entry *stcp1;
     struct stgcat_entry *stcp2;
     int bindiff;
{
  int i;

  if (stcp1 == NULL || stcp2 == NULL) {
    return(-2);
  }

  if (bindiff != 0) {
    return(memcmp(stcp1,stcp2,sizeof(struct stgcat_entry)));
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
     struct stgcat_entry *stcp1;
     struct stgcat_entry *stcp2;
{
  int i;

  if (stcp1 == NULL || stcp2 == NULL) {
    return;
  }

  printf("----------------------------------------------------------------\n");
  printf("%10s : %10s ... %10s ... %10s ... %10s\n","What","Catalog","Cdb",
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
  printf("... Total dump of entry in catalog and in Cdb:\n");
  stgconvert_dump2((char *) stcp1,(char *) stcp2, sizeof(struct stgcat_entry));
}

int stppcmp(stpp1,stpp2,bindiff)
     struct stgpath_entry *stpp1;
     struct stgpath_entry *stpp2;
     int bindiff;
{
  if (stpp1 == NULL || stpp2 == NULL) {
    return(-2);
  }

  if (bindiff != 0) {
    return(memcmp(stpp1,stpp2,sizeof(struct stgpath_entry)));
  }

  if (stpp1->reqid != stpp2->reqid ||
      strcmp(stpp1->upath,stpp2->upath) != 0) {
    return(-1);
  }

  /* OKAY */
  return(0);
}

int stgdb_stcpcmp(p1,p2)
     CONST void *p1;
     CONST void *p2;
{
  struct stgcat_entry *stcp1 = (struct stgcat_entry *) p1;
  struct stgcat_entry *stcp2 = (struct stgcat_entry *) p2;

  if (stcp1->c_time < stcp2->c_time) {
    return(-1);
  } else if (stcp1->c_time > stcp2->c_time) {
    return(1);
  } else {
    if (stcp1->reqid < stcp2->reqid) {
      return(-1);
    } else if (stcp1->reqid > stcp2->reqid) {
      return(1);
    } else {
      if (warns != 0) {
        printf("### Warning : two elements in stgcat have same c_time (%d) and reqid (%d)\n",
               (int) stcp1->c_time, (int) stcp1->reqid);
      }
      return(0);
    }
  }
}

int stgdb_stppcmp(p1,p2)
     CONST void *p1;
     CONST void *p2;
{
  struct stgpath_entry *stpp1 = (struct stgpath_entry *) p1;
  struct stgpath_entry *stpp2 = (struct stgpath_entry *) p2;

  if (stpp1->reqid < stpp2->reqid) {
    return(-1);
  } else if (stpp1->reqid > stpp2->reqid) {
    return(1);
  } else {
    if (warns != 0) {
      printf("### Warning : two elements in stgpath have same reqid (%d)\n",(int) stpp1->reqid);
    }
    return(0);
  }
}

void stppprint(stpp1,stpp2)
     struct stgpath_entry *stpp1;
     struct stgpath_entry *stpp2;
{
  if (stpp1 == NULL || stpp2 == NULL) {
    return;
  }

  printf("----------------------------------------------------------------\n");
  printf("%10s : %10s ... %10s ... %10s ... %10s\n","What","Catalog","Cdb",
         "Status","Bin Status");
  printf("----------------------------------------------------------------\n");
  STPCMP_VAL(reqid);
  STPCMP_STRING(upath);
  printf("... Total dump of entry in catalog and in Cdb:\n");
  stgconvert_dump2((char *) stpp1,(char *) stpp2, sizeof(struct stgpath_entry));
}

void stgconvert_dump(buffer, size)
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

void stgconvert_dump2(buffer1, buffer2, size)
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
