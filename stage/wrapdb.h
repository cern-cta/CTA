#ifndef __wrapdb_h
#define __wrapdb_h

#include <sys/types.h>
#include <Cdb_api.h>
#include "stage.h"

/* wrap functions prototypes */
#define    wrapCdb_fetch(a,b,c,d,e) fullwrapCdb_fetch(__FILE__,__LINE__,a,b,c,d,e)
#define    wrapCdb_delete(a,b,c) fullwrapCdb_delete(__FILE__,__LINE__,a,b,c)
#define    wrapCdb_altkey_flag(a,b) fullwrapCdb_altkey_flag(__FILE__,__LINE__,a,b)
#define    wrapCdb_store(a,b,c,d,e,f) fullwrapCdb_store(__FILE__,__LINE__,a,b,c,d,e,f)
#define    wrapCdb_nextrec(a,b,c,d,e) fullwrapCdb_nextrec(__FILE__,__LINE__,a,b,c,d,e)
#define    wrapCdb_altkey_status_indelete(a,b,c,d,e) fullwrapCdb_altkey_status_indelete(__FILE__,__LINE__,a,b,c,d,e)

#if defined(__STDC__)
extern int fullwrapCdb_fetch(char *, int, db_fd **, int, void **, size_t *, int);
extern int fullwrapCdb_delete(char *, int, db_fd **, int, int);
extern int fullwrapCdb_altkey_flag(char *, int, struct stgcat_entry *, int);
extern int fullwrapCdb_store(char *, int, db_fd **, int, void *, size_t, int, int);
extern int fullwrapCdb_nextrec(char *, int, db_fd **, char **, void **, size_t *, int);
extern int fullwrapCdb_altkey_status_indelete(char *, int, db_fd **, char *, int, int, int);
#else
extern int fullwrapCdb_fetch();
extern int fullwrapCdb_delete();
extern int fullwrapCdb_altkey_flag();
extern int fullwrapCdb_store();
extern int fullwrapCdb_nextrec();
extern int fullwrapCdb_altkey_status_indelete();
#endif /* __STDC__ */

#endif /* __wrapdb_h */


