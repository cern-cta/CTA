#include "wrapdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

/* Required external variables: the two DB databases */
extern db_fd **db_stgcat;
extern db_fd **db_stgpath;

/* External prototypes (cd db directory) */
#if defined(__STDC__)
extern int count_digits(int, unsigned int);
#else
extern int count_digits();
#endif

/* ------------------------------------- */
/* Subroutine: fullwrapCdb_fetch         */
/* ------------------------------------- */
int fullwrapCdb_fetch(file, line, fd, reqid, data, size, fatal)
     char *file;
     int line;
     db_fd **fd;
     int reqid;
     void **data;
     size_t *size;
     int fatal;
{
  char *key;
  int  status;

#ifdef TEST
  stglogit("fullwrapCdb_fetch",
           "Fetch of reqid=%d on socket %d called at %s:%d\n",
           reqid,fd->sockfd,file,line);
#endif

  if ((key = malloc(1 + count_digits(reqid, 10))) == NULL) {
#ifdef TEST
    stglogit("fullwrapCdb_fetch",
             "Fetch of reqid=%d called at %s:%d : malloc error at %s:%d\n",
             reqid,file,line,__FILE__,__LINE__);
#endif
    return(1);
  }
  sprintf(key,"%d",reqid);

  status = Cdb_fetch(fd, key, data, size);
  free(key);

#ifdef TEST
  if (status == 0 && fd == db_stgcat && data != NULL && *data != NULL) {
    struct stgcat_entry *stcp;

    stcp = (struct stgcat_entry *) *data;
    stglogit("fullwprapCdb_fetch",
             "Fetch of reqid=%d called at %s:%d : stcp->[u1.t.vid[0],u1.t.vsn[0],uid,gid] = [%s,%s,%d,%d]\n",
             reqid,file,line,stcp->u1.t.vid[0],stcp->u1.t.vsn[0],stcp->uid,stcp->gid);
  }
#endif

  if (status != 0 && fatal != 0) {
    stglogit("fullwrapCdb_fetch",
             "Fetch of reqid=%d called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
             reqid,file,line,errno,db_strerror(errno),__FILE__,__LINE__);
    exit(EXIT_FAILURE);
  }

#ifdef TEST
  if (status != 0) {
    stglogit("fullwrapCdb_fetch",
             "Fetch of reqid=%d called at %s:%d : return(1) at %s:%d\n",
             reqid,file,line,__FILE__,__LINE__);
  }
#endif

  return(status);
}

/* ------------------------------------- */
/* Subroutine: fullwrapCdb_nextrec       */
/* ------------------------------------- */
int fullwrapCdb_nextrec(file, line, fd, key, data, size, fatal)
     char *file;
     int line;
     db_fd **fd;
     char **key;
     void **data;
     size_t *size;
     int fatal;
{
  int  status;

#ifdef TEST
  stglogit("fullwrapCdb_nextrec",
           "Nextrec on socket %d called at %s:%d\n",
           fd->sockfd,file,line);
#endif

  status = Cdb_nextrec(fd, key, data, size);

  if (status != 0 && fatal != 0) {
    stglogit("fullwrapCdb_nextrec",
             "Nextrec called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
             file,line,errno,db_strerror(errno),__FILE__,__LINE__);
    exit(EXIT_FAILURE);
  }

#ifdef TEST
  if (status != 0) {
    stglogit("fullwrapCdb_nextrec",
             "Nextrec called at %s:%d : return(1) at %s:%d\n",
             file,line,__FILE__,__LINE__);
  }
#endif

  return(status);

}

/* ------------------------------------- */
/* Subroutine: fullwrapCdb_delete        */
/* ------------------------------------- */
int fullwrapCdb_delete(file, line, fd, reqid, fatal)
     char *file;
     int line;
     db_fd **fd;
     int reqid;
     int fatal;
{
  char *key;
  int  status;

#ifdef TEST
  stglogit("fullwrapCdb_delete",
           "Deletion of reqid=%d on socket %d called at %s:%d\n",
           reqid,fd->sockfd,file,line);
#endif

  if ((key = malloc(1 + count_digits(reqid, 10))) == NULL) {
#ifdef TEST
    stglogit("fullwrapCdb_delete",
             "Deletion of reqid=%d called at %s:%d : malloc error at %s:%d\n",
             reqid,file,line,__FILE__,__LINE__);
#endif
    return(1);
  }
  sprintf(key,"%d",reqid);
  status = Cdb_delete(fd,key);
  free(key);
  if (status != 0 && fatal != 0) {
    stglogit("fullwrapCdb_delete",
             "Deletion of reqid=%d called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
             reqid,file,line,errno,db_strerror(errno),__FILE__,__LINE__);
    exit(EXIT_FAILURE);
  }
  return(status);
}

/* ------------------------------------- */
/* Subroutine: fullwrapCdb_altkey_flag   */
/* ------------------------------------- */
int fullwrapCdb_altkey_flag(file, line, stcp, fatal)
     char *file;
     int line;
     struct stgcat_entry *stcp;
     int fatal;
{
  int i;
  int status;

  status = 0;

  /* --------------- */
  /* poolname        */
  /* --------------- */
  if (! stcp->poolname[0]) {
    if (Cdb_altkey_flag(db_stgcat,"poolname",DB_ALTKEY_DISABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }
  } else {
    if (Cdb_altkey_flag(db_stgcat,"poolname",DB_ALTKEY_ENABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }
  }

  /* --------------- */
  /* status          */
  /* --------------- */
  if (stcp->status == 0) {
    if (Cdb_altkey_flag(db_stgcat,"status",DB_ALTKEY_DISABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }
  } else {
    if (Cdb_altkey_flag(db_stgcat,"status",DB_ALTKEY_ENABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }
  }

  /* --------------- */
  /* ipath           */
  /* --------------- */
  if (! stcp->ipath[0]) {
    if (Cdb_altkey_flag(db_stgcat,"ipath",DB_ALTKEY_DISABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }
  } else {
    if (Cdb_altkey_flag(db_stgcat,"ipath",DB_ALTKEY_ENABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }
  }

  /* --------------- */
  /* 't' entry       */
  /* --------------- */
  if (stcp->t_or_d == 't') {
    if (Cdb_altkey_flag(db_stgcat,"u1.d.xfile",DB_ALTKEY_DISABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }
    if (Cdb_altkey_flag(db_stgcat,"u1.m.xfile",DB_ALTKEY_DISABLE) != 0) {
      if (fatal != 0) {
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
      ++status;
    }

    /* Change the vid[] flags */
    for (i = 0; i < MAXVSN; i++) {
      char *newkey;
      int   thisstatus;
      if ((newkey = (char *) malloc(strlen("u1.t.vid") + count_digits(i,10) + 1)) == NULL) {
#ifdef TEST
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : malloc error at %s:%d\n",
                 file,line,__FILE__,__LINE__);
#endif
        return(1);
      }
      strcpy(newkey,"u1.t.vid");
      sprintf(newkey + strlen("u1.t.vid"),"%d",i);
      if (strlen(stcp->u1.t.vid[i]) > 0)
        thisstatus = Cdb_altkey_flag(db_stgcat,newkey,DB_ALTKEY_ENABLE);
      else
        thisstatus = Cdb_altkey_flag(db_stgcat,newkey,DB_ALTKEY_DISABLE);
      free(newkey);
      if (thisstatus != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
    }
    /* Disable the non-needed vsn[] flags */
    for (i = 0; i < MAXVSN; i++) {
      char *newkey;
      int   thisstatus;
      if ((newkey = (char *) malloc(strlen("u1.t.vsn") + count_digits(i,10) + 1)) == NULL) {
#ifdef TEST
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : malloc error at %s:%d\n",
                 file,line,__FILE__,__LINE__);
#endif
        return(1);
      }
      strcpy(newkey,"u1.t.vsn");
      sprintf(newkey + strlen("u1.t.vsn"),"%d",i);
      if (strlen(stcp->u1.t.vsn[i]) > 0)
        thisstatus = Cdb_altkey_flag(db_stgcat,newkey,DB_ALTKEY_ENABLE);
      else
        thisstatus = Cdb_altkey_flag(db_stgcat,newkey,DB_ALTKEY_DISABLE);
      free(newkey);
      if (thisstatus != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
    }
  } else {
    if (stcp->t_or_d == 'm') {
      /* --------------- */
      /* 'm' entry       */
      /* --------------- */
      if (Cdb_altkey_flag(db_stgcat,"u1.m.xfile",DB_ALTKEY_ENABLE) != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
      if (Cdb_altkey_flag(db_stgcat,"u1.d.xfile",DB_ALTKEY_DISABLE) != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
    } else if (stcp->t_or_d == 'd') {
      /* --------------- */
      /* 'd' entry       */
      /* --------------- */
      if (Cdb_altkey_flag(db_stgcat,"u1.m.xfile",DB_ALTKEY_DISABLE) != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
      if (Cdb_altkey_flag(db_stgcat,"u1.d.xfile",DB_ALTKEY_ENABLE) != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
    } else {
      /* --------------- */
      /* '?' entry       */
      /* --------------- */
      if (Cdb_altkey_flag(db_stgcat,"u1.m.xfile",DB_ALTKEY_DISABLE) != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
      if (Cdb_altkey_flag(db_stgcat,"u1.d.xfile",DB_ALTKEY_DISABLE) != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
    }
    /* ------------------------------------------------------- */
    /* not a 't' entry : disable all the vid[] and vsn[] flags */
    /* ------------------------------------------------------- */
    for (i = 0; i < MAXVSN; i++) {
      char *newkey;
      int   thisstatus;
      if ((newkey = (char *) malloc(strlen("u1.t.vid") + count_digits(i,10) + 1)) == NULL) {
#ifdef TEST
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : malloc error at %s:%d\n",
                 file,line,__FILE__,__LINE__);
#endif
        return(1);
      }
      strcpy(newkey,"u1.t.vid");
      sprintf(newkey + strlen("u1.t.vid"),"%d",i);
      thisstatus = Cdb_altkey_flag(db_stgcat,newkey,DB_ALTKEY_DISABLE);
      free(newkey);
      if (thisstatus != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
    }
    /* Disable all the vsn[] flags */
    for (i = 0; i < MAXVSN; i++) {
      char *newkey;
      int   thisstatus;
      if ((newkey = (char *) malloc(strlen("u1.t.vsn") + count_digits(i,10) + 1)) == NULL) {
#ifdef TEST
        stglogit("fullwrapCdb_altkey_flag",
                 "Altkey_flag called at %s:%d : malloc error at %s:%d\n",
                 file,line,__FILE__,__LINE__);
#endif
        return(1);
      }
      strcpy(newkey,"u1.t.vsn");
      sprintf(newkey + strlen("u1.t.vsn"),"%d",i);
      thisstatus = Cdb_altkey_flag(db_stgcat,newkey,DB_ALTKEY_DISABLE);
      free(newkey);
      if (thisstatus != 0) {
        if (fatal != 0) {
          stglogit("fullwrapCdb_altkey_flag",
                   "Altkey_flag called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                   file,line,errno,db_strerror(errno),__FILE__,__LINE__);
          exit(EXIT_FAILURE);
        }
        ++status;
      }
    }
  }
#ifdef TEST
  stglogit("fullwrapCdb_altkey_flag",
           "Altkey_flag global status : %d\n",
           status);
#endif
  if (fatal != 0) {
    return(status);
  }
  return(0);
}

/* ------------------------------------- */
/* Subroutine: fullwrapCdb_store         */
/* ------------------------------------- */
int fullwrapCdb_store(file, line, fd, reqid, data, size, flag, fatal)
     char *file;
     int line;
     db_fd **fd;
     int reqid;
     void *data;
     size_t size;
     int flag;
     int fatal;
{
  char *key;
  int  status;

#ifdef TEST
  stglogit("fullwrapCdb_store",
           "Storage of reqid=%d called at %s:%d\n",
           reqid,file,line);
#endif

  status = 0;

  if ((key = malloc(1 + count_digits(reqid, 10))) == NULL) {
#ifdef TEST
    stglogit("fullwrapCdb_store",
             "Storage of reqid=%d called at %s:%d : malloc error at %s:%d\n",
             reqid,file,line,__FILE__,__LINE__);
#endif
    return(1);
  }

  sprintf(key,"%d",reqid);

  /* In case of an stgcat update, we change some flags vs. alternate key t_or_d */
  if (fd == db_stgcat)
    if (wrapCdb_altkey_flag((struct stgcat_entry *) data, 0) != 0) {
#ifdef TEST
      stglogit("fullwrapCdb_store",
               "Storage of reqid=%d called at %s:%d : Altkey_flag error at %s:%d\n",
               reqid,file,line,__FILE__,__LINE__);
#endif
      if (fatal) {
        stglogit("fullwrapCdb_store",
                 "Storage of reqid=%d called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
                 reqid,file,line,errno,db_strerror(errno),__FILE__,__LINE__);
        exit(EXIT_FAILURE);
      }
    }
  
#ifdef TEST
  stglogit("fullwrapCdb_store",
           "Calling Cdb_store(fd,key=%s,data,size=%d,flag=%d)\n",
           key,(int) size,(int) flag);
#endif
  status = Cdb_store(fd,key,data,size,flag);
#ifdef TEST
  if (fd == db_stgcat && data != NULL) {
    struct stgcat_entry *stcp;

    stcp = (struct stgcat_entry *) data;
    stglogit("fullwprapCdb_store",
             "Storage of reqid=%d called at %s:%d : stcp->[u1.t.vid[0],u1.t.vsn[0],uid,gid] = [%s,%s,%d,%d]\n",
             reqid,file,line,stcp->u1.t.vid[0],stcp->u1.t.vsn[0],stcp->uid,stcp->gid);
  }
#endif
#ifdef TEST
  stglogit("fullwrapCdb_store",
           "status %d from Cdb_store(fd,key=%s,data,size=%d,flag=%d)\n",
           status,key,(int) size,(int) flag);
#endif

  if (status != 0 && fatal != 0) {
    stglogit("fullwrapCdb_store",
             "Storage of reqid=%d called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
             reqid,file,line,errno,db_strerror(errno),__FILE__,__LINE__);
    exit(EXIT_FAILURE);
  }

  free(key);
  return(status);
} 

/* ---------------------------------------------- */
/* Subroutine: fullwrapCdb_altkey_status_indelete */
/* ---------------------------------------------- */
int fullwrapCdb_altkey_status_indelete(file, line, fd, altkey, status, reqid, fatal)
     char *file;
     int line;
     db_fd **fd;
     char *altkey;
     int status;
     int reqid;
     int fatal;
{
  char *key     = NULL;
  char *primary = NULL;

#ifdef TEST
  stglogit("fullwrapCdb_altkey_status_indelete",
           "Deletion of reqid=%d, for status %d, in Alt.DB %s called at %s:%d\n",
           reqid,status,altkey,file,line);
#endif

  if ((key = malloc(1 + count_digits(status, 10))) == NULL) {
#ifdef TEST
    stglogit("fullwrapCdb_altkey_status_indelete",
             "Deletion of reqid=%d, for status %d, in Alt.DB %s called at %s:%d : malloc error at %s:%d\n",
             reqid,status,altkey,file,line,__FILE__,__LINE__);
#endif
    return(1);
  }
  sprintf(key,"%d",status);

  if ((primary = malloc(1 + count_digits(reqid, 10))) == NULL) {
#ifdef TEST
    stglogit("fullwrapCdb_altkey_status_indelete",
             "Deletion of reqid=%d, for status %d, in Alt.DB %s called at %s:%d : malloc error at %s:%d\n",
             reqid,status,altkey,file,line,__FILE__,__LINE__);
#endif
    free(key);
    return(1);
  }
  sprintf(primary,"%d",reqid);

  if (Cdb_altkey_indelete(fd, altkey, key, primary)) {
    if (fatal) {
      stglogit("fullwrapCdb_altkey_status_indelete",
               "Deletion of reqid=%d, for status %d, in Alt.DB %s called at %s:%d : Error %d (%s) : exit(EXIT_FAILURE) at %s:%d\n",
               reqid,status,altkey,file,line,errno,db_strerror(errno),__FILE__,__LINE__);
      free(key);
      free(primary);
      exit(EXIT_FAILURE);
    } else {
#ifdef TEST
      stglogit("fullwrapCdb_altkey_status_indelete",
               "Deletion of reqid=%d, for status %d, in Alt.DB %s called at %s:%d : return(1) at %s:%d\n",
               reqid,status,altkey,file,line,__FILE__,__LINE__);
#endif
      free(key);
      free(primary);
      return(1);
    }
  } else {
    free(key);
    free(primary);
    return(0);
  }
}
