/*
 * $Id: stgconvert.c,v 1.2 1999/12/02 14:38:44 jdurand Exp $
 */

/*
 * Conversion Program between SHIFT stager catalog and CASTOR/Cdb stager database
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
#include <log.h>

/* =============================================================== */
/* Local headers for threads : to be included before ANYTHING else */
/* =============================================================== */
#include "osdep.h"
#include "Cdb_api.h"                /* CASTOR Cdb Interface */
#include "Cstage_db.h"              /* Generated STAGE/Cdb header */
#include "stage.h"                  /* SHIFT's header */
#include "serrno.h"                 /* CASTOR's serrno */

/* =============== */
/* Local variables */
/* =============== */
static char *sccsid = "@(#)$RCSfile: stgconvert.c,v $ $Revision: 1.2 $ $Date: 1999/12/02 14:38:44 $ CERN IT-PDP/DM Jean-Damien Durand / Jean-Philippe Baud";

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

#define CASTOR_TO_SHIFT -1
#define SHIFT_CMP_CASTOR 0
#define SHIFT_TO_CASTOR  1

#define CDB_USERNAME "Cdb_Stage_User"
#define CDB_PASSWORD "Cdb_Stage_Password"

/* =================== */
/* Internal prototypes */
/* =================== */
void stgconvert_usage _PROTO(());
int stcpcmp _PROTO((struct stgcat_entry *, struct stgcat_entry *));

int main(argc,argv)
     int argc;
     char **argv;
{
  /* For options */
  extern char *optarg;
  extern int optind, opterr, optopt;
  int errflg;
  int c;
  int convert_direction = 0;         /* 1 : SHIFT -> CASTOR, -1 : CASTOR -> SHIFT */
  int help = 0;                      /* 1 : help wanted */
  char *stgcat = NULL;               /* SHIFT's stager catalog path */
  int stgcat_fd = -1;
  char *stgpath = NULL;              /* SHIFT's stager link catalog path */
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

  while ((c = getopt(argc,argv,"c:Cg:hl:Lu:p:t:v")) != EOF) {
    switch (c) {
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

  if (convert_direction != SHIFT_TO_CASTOR && 
      convert_direction != CASTOR_TO_SHIFT &&
      convert_direction != SHIFT_CMP_CASTOR) {
    printf("?? -g option is REQUIRED and HAS to be either %d (SHIFT->CASTOR), %d (CASTOR->SHIFT) or %d (SHIFT cmp CASTOR), nothing else.\n",
            SHIFT_TO_CASTOR,CASTOR_TO_SHIFT,SHIFT_CMP_CASTOR);
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

  if (convert_direction == CASTOR_TO_SHIFT) {
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

  /* If the user said CASTOR -> SHIFT conversion and one of those files is of length > 0   */
  /* he will overwrite existing non-zero length files. We then check if that's really what */
  /* he wants to do.                                                                       */
  if (convert_direction == CASTOR_TO_SHIFT) {
    if ((no_stgcat == 0  && stgcat_statbuff.st_size > 0) || 
        (no_stgpath == 0 && stgpath_statbuff.st_size > 0)) {
      int answer;

      printf("### WARNING : You are going to truncate a SHIFT catalog that is of non-zero length\n");
      if (no_stgcat == 0) {
        printf("### Current %s length  : 0x%lx\n",stgcat,(unsigned long) stgcat_statbuff.st_size);
      }
      if (no_stgpath == 0) {
        printf("### Current %s length : 0x%lx\n",stgpath,(unsigned long) stgpath_statbuff.st_size);
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

  if (convert_direction == SHIFT_TO_CASTOR) {
    /* ========================== */
    /* SHIFT -> CASTOR conversion */
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
        if (read(stgcat_fd,stcs,stgcat_statbuff.st_size) != stgcat_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stce = stcs;
      stce += stgcat_statbuff.st_size;
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
        if (read(stgpath_fd,stps,stgpath_statbuff.st_size) != stgpath_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stpe = stps;
      stpe += stgpath_statbuff.st_size;
      for (stpp = stps; stpp < stpe; stpp++) {
        if (stpp->reqid == 0) {
          break;
        }
        ++nstpp;
      }
      
    }


    if (no_stgcat == 0) {
      printf("\n*** CONVERTING stgcat CATALOG ***\n\n");
      
      /* We loop on the SHIFT's stgcat catalog */
      /* ------------------------------------- */
      i = 0;
      for (stcp = stcs; stcp < stce; stcp++) {
        if (stcp->reqid == 0) {
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
            printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_tape\" [VID[0]=%s,FSEQ=%s] at offset 0x%lx\n",
                   i,nstcp,tape.reqid,tape.vid[0],tape.fseq,(unsigned long) Cdb_offset);
          }
          break;
        case 'd':
          if (Cdb_insert(&Cdb_db,"stgcat_disk",NULL,&disk,&Cdb_offset) != 0) {
            printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_disk\" (%s)\n"
                   ,disk.reqid
                   ,sstrerror(serrno));
          } else {
            printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_disk\" [xfile=%s] at offset 0x%lx\n",
                   i,nstcp,disk.reqid,disk.xfile,(unsigned long) Cdb_offset);
          }
          break;
        case 'm':
          if (Cdb_insert(&Cdb_db,"stgcat_hsm",NULL,&hsm,&Cdb_offset) != 0) {
            printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_hsm\" (%s)\n"
                   ,hsm.reqid
                   ,sstrerror(serrno));
          } else {
            printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_hsm\" [xfile=%s] at offset 0x%lx\n",
                   i,nstcp,hsm.reqid,hsm.xfile,(unsigned long) Cdb_offset);
          }
          break;
        case 'a':
          if (Cdb_insert(&Cdb_db,"stgcat_alloc",NULL,&alloc,&Cdb_offset) != 0) {
            printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_hsm\" (%s)\n"
                   ,alloc.reqid
                   ,sstrerror(serrno));
          } else {
            printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_alloc\" [xfile=%s] at offset 0x%lx\n",
                   i,nstcp,hsm.reqid,hsm.xfile,(unsigned long) Cdb_offset);
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

    /* We loop on the SHIFT's stgpath catalog */
    /* -------------------------------------- */
      i = 0;
      for (stpp = stps; stpp < stpe; stpp++) {
        if (stpp->reqid == 0) {
          break;
        }
        ++i;
        if (maxstpp >= 0 && i > maxstpp) {
          break;
        }
        if (stpp2Cdb(stpp,&link) != 0) {
          printf("### stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
        }

        /* We insert this record */
        if (Cdb_insert(&Cdb_db,"stgcat_link",NULL,&link,&Cdb_offset) != 0) {
          printf("### Cannot insert entry with reqid = %d in Cdb's table \"stgcat_link\" (%s)\n"
                 ,stcp->reqid
                 ,sstrerror(serrno));
        } else {
          printf("--> (%6d/%6d) reqid = %d inserted in \"stgcat_link\" [upath=%s] at offset 0x%lx\n",
                 i,nstpp,link.reqid,link.upath,(unsigned long) Cdb_offset);
        }
      }
    }

  } else if (convert_direction == CASTOR_TO_SHIFT) {

    /* ========================== */
    /* CASTOR -> SHIFT conversion */
    /* ========================== */
    
    int i = 0;

    if (no_stgcat == 0) {
      /* We truncate the files */
      if (ftruncate(stgcat_fd, (size_t) 0) != 0) {
        printf("### stat on %s error, %s\n"
               ,stgcat
               ,strerror(errno));
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
    }
    if (no_stgpath == 0) {
      if (ftruncate(stgpath_fd, (size_t) 0) != 0) {
        printf("### stat on %s error, %s\n"
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
      if (Cdb_dump_start(&Cdb_db,"stgcat_tape","stgcat_tape_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          printf("--> (%6d) reqid = %d OK\n",++i,thisstcp.reqid);
        }

      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_tape","stgcat_tape_per_reqid") != 0) {
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

      if (Cdb_dump_start(&Cdb_db,"stgcat_disk","stgcat_disk_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          printf("--> (%6d) reqid = %d OK\n",++i,thisstcp.reqid);
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_disk","stgcat_disk_per_reqid") != 0) {
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

      if (Cdb_dump_start(&Cdb_db,"stgcat_hsm","stgcat_hsm_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          printf("--> (%6d) reqid = %d OK\n",++i,thisstcp.reqid);
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_hsm","stgcat_hsm_per_reqid") != 0) {
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

      if (Cdb_dump_start(&Cdb_db,"stgcat_alloc","stgcat_alloc_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        if (write(stgcat_fd,&thisstcp,sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
          printf("### write() error on %s (%s)\n",stgcat,strerror(errno));
        } else {
          printf("--> (%6d) reqid = %d OK\n",++i,thisstcp.reqid);
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_alloc","stgcat_alloc_per_reqid") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_alloc\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }
    }

  no_alloced_dump:

    if (no_stgpath == 0) {
      /* We ask for a dump of stgpath master table from Cdb */
      /* -------------------------------------------------- */
      printf("\n*** DUMPING stgcat_link TABLE ***\n\n");
      
      i = 0;
      
      if (Cdb_dump_start(&Cdb_db,"stgcat_link","stgcat_link_per_reqid") != 0) {
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
            printf("--> (%6d) reqid = %d converted back\n",++i,thisstpp.reqid);
          }
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_link","stgcat_link_per_reqid") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_link\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }
    }

  } else if (convert_direction == SHIFT_CMP_CASTOR) {
    /* =========================== */
    /* SHIFT cmp CASTOR conversion */
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
        if (read(stgcat_fd,stcs,stgcat_statbuff.st_size) != stgcat_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stce = stcs;
      stce += stgcat_statbuff.st_size;
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
        if (read(stgpath_fd,stps,stgpath_statbuff.st_size) != stgpath_statbuff.st_size) {
          printf("### read error, %s\n",strerror(errno));
          rc = EXIT_FAILURE;
          goto stgconvert_return;
        }
      }
      stpe = stps;
      stpe += stgpath_statbuff.st_size;
      for (stpp = stps; stpp < stpe; stpp++) {
        if (stpp->reqid == 0) {
          break;
        }
        ++nstpp;
      }
    }

    if (no_stgcat == 0) {
      printf("\n*** COMPARING stgcat CATALOG for CASTOR and SHIFT ***\n\n");

      if (t_or_d != '\0' && t_or_d != 't') {
        goto no_tape_cmp;
      }

      printf("\n*** DUMPING stgcat_tape TABLE ***\n\n");

    /* We ask for a dump of tape table from Cdb */
    /* ---------------------------------------- */
      if (Cdb_dump_start(&Cdb_db,"stgcat_tape","stgcat_tape_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
          }
        }

        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",++i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          printf("... (%6d/%6d) reqid = %d Comparison OK\n",++i,nstcp,thisstcp.reqid);
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",++i,nstcp,thisstcp.reqid);
          global_stgcat_cmp_status = -1;
        }

      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_tape","stgcat_tape_per_reqid") != 0) {
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
      if (Cdb_dump_start(&Cdb_db,"stgcat_disk","stgcat_disk_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
          }
        }
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",++i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          printf("... (%6d/%6d) reqid = %d Comparison OK\n",++i,nstcp,thisstcp.reqid);
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",++i,nstcp,thisstcp.reqid);
          global_stgcat_cmp_status = -1;
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_disk","stgcat_disk_per_reqid") != 0) {
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
      if (Cdb_dump_start(&Cdb_db,"stgcat_hsm","stgcat_hsm_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
          }
        }
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",++i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          printf("... (%6d/%6d) reqid = %d Comparison OK\n",++i,nstcp,thisstcp.reqid);
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",++i,nstcp,thisstcp.reqid);
          global_stgcat_cmp_status = -1;
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_hsm","stgcat_hsm_per_reqid") != 0) {
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
      if (Cdb_dump_start(&Cdb_db,"stgcat_alloc","stgcat_alloc_per_reqid") != 0) {
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
          printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
          continue;
        }

        for (stcp = stcs; stcp < stce; stcp++) {
          if (stcp->reqid == 0) {
            break;
          }
          if (stcp->reqid == thisstcp.reqid) {
            cmp_status = stcpcmp(stcp,&thisstcp);
            /* We put reqid to its minus version so that we will know */
            /* that this value has been previously scanned... */
            stcp->reqid *= -1;
          }
        }
        if (cmp_status == -2) {
          printf("### (%6d/%6d) reqid = %d is NOT IN %s\n",++i,nstcp,thisstcp.reqid,stgcat);
          global_stgcat_cmp_status = -1;
        } else if (cmp_status == 0) {
          printf("... (%6d/%6d) reqid = %d Comparison OK\n",++i,nstcp,thisstcp.reqid);
        } else {
          printf("### (%6d/%6d) reqid = %d is NOT THE SAME\n",++i,nstcp,thisstcp.reqid);
          global_stgcat_cmp_status = -1;
        }
      }

      if (Cdb_dump_end(&Cdb_db,"stgcat_alloc","stgcat_alloc_per_reqid") != 0) {
        printf("### Cdb_dump_end error on table \"stgcat_alloc\" (%s)\n",sstrerror(serrno));
        if (Cdb_error(&Cdb_session,&error) == 0) {
          printf("--> more info:\n%s",error);
        }
      }

    }

    no_alloced_cmp:
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
         "  This program will convert from and to the SHIFT stager catalog\n"
         "and the CASTOR interfaced to Cdb database.\n"
         "\n"
         "Options are:\n"
         "  -h                Print this help and exit\n"
         "  -C                Do nothing about stgcat\n"
         "  -c <number>       maximum number to convert from SHIFT to CASTOR for stgcat\n"
         "  -g <number>       Convert direction, where\n"
         "                    -g %2d means: SHIFT -> CASTOR\n"
         "                    -g %2d means: SHIFT cmp CASTOR\n"
         "                    -g %2d means: CASTOR -> SHIFT\n"
         "  -l <number>       maximum number to convert from SHIFT to CASTOR for stgpath\n"
         "  -L                Do nothing about stgpath\n"
         "  -u                Cdb username. Defaults to \"%s\"\n"
         "  -p                Cdb password. Defaults to \"%s\"\n"
         "  -t <type>         Restrict this program to data of type:\n"
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
         "       If you specify SHIFT -> CASTOR conversion you are suggested to reset the\n"
         "       \"stage\" database inside Cdb. This is achieved by removing the directory\n"
         "       /usr/spool/db/stage/.\n"
         "\n"
         " Example of SHIFT->CASTOR convertion:\n"
         "  stgconvert -g %2d /usr/spool/stage/stgcat /usr/spool/stage/stgpath\n"
         "\n"
         " Example of SHIFT/CASTOR comparison:\n"
         "  stgconvert -g %2d /usr/spool/stage/stgcat /usr/spool/stage/stgpath\n"
         "\n"
         " Example of CASTOR->SHIFT convertion:\n"
         "  stgconvert -g %2d /usr/spool/stage/stgcat.new /usr/spool/stage/stgpath.new\n"
         "\n"
         "Comments to castor-support@listbox.cern.ch\n"
         "\n"
         ,SHIFT_TO_CASTOR,SHIFT_CMP_CASTOR,CASTOR_TO_SHIFT,CDB_USERNAME,CDB_PASSWORD
         ,SHIFT_TO_CASTOR,SHIFT_CMP_CASTOR,CASTOR_TO_SHIFT
         );
}

int stcpcmp(stcp1,stcp2)
     struct stgcat_entry *stcp1;
     struct stgcat_entry *stcp2;
{
  int i;

  if (stcp1 == NULL || stcp2 == NULL) {
    return(-2);
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

