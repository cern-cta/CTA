#ifndef __rm_macros_h
#define __rm_macros_h

#include <stdlib.h>                /* For malloc */
#include "rm_struct.h"
#include "rm_constants.h"
#include "rm_limits.h"
#include "Castor_limits.h"
#include "marshall.h"
#include "serrno.h"

#define RM_NB_ELEMENTS(a) (sizeof(a)/sizeof((a)[0]))

#define INIT_HOST(thishost) {                               \
	(thishost)->s_addr = clientAddress.sin_addr.s_addr;     \
	(thishost)->nbrestart = nbrestart;                      \
	strncpy((thishost)->host,rec_host_name,CA_MAXHOSTNAMELEN); \
	(thishost)->hostlen = strlen((thishost)->host);         \
	strcpy((thishost)->state,"Idle");                       \
}

#ifdef LSF
#define INIT_JOB(thisjob) {                      \
	(thisjob)->update = (u_signed64) time(NULL); \
	(thisjob)->wclimit = 864000;                 \
	(thisjob)->tasks = 1;                        \
	(thisjob)->queuetime = (thisjob)->update;    \
	(thisjob)->openflags = RM_O_RDWR;            \
}
#else
#define INIT_JOB(thisjob) {                      \
	strcpy((thisjob)->state,"Idle");             \
	(thisjob)->update = (u_signed64) time(NULL); \
	(thisjob)->wclimit = 864000;                 \
	(thisjob)->tasks = 1;                        \
	(thisjob)->queuetime = (thisjob)->update;    \
	(thisjob)->openflags = RM_O_RDWR;            \
	(thisjob)->subreqid = -1;                    \
}
#endif
#define ADD_NEW_TAPE_FEATURE(to,vid) { \
	if (to == NULL) { \
		if ((to = (char *) malloc(strlen(vid)+1)) == NULL) { \
			log(LOG_ERR,"%s: malloc error : %s\n", func, strerror(errno)); \
		} else { \
			strcpy(to,vid); \
		} \
	} else { \
		char *to2; \
		if ((to2 = (char *) realloc(to,strlen(to)+1+strlen(vid)+1)) == NULL) { \
			log(LOG_ERR,"%s: realloc error : %s\n", func, strerror(errno)); \
		} else { \
			to = to2; \
			strcat(to,":"); \
			strcat(to,vid); \
		} \
	} \
}

#define marshall_RMNODE(p, rmnode) {                                     \
	marshall_STRING(p, (rmnode)->host);                                  \
	marshall_HYPER(p, (rmnode)->hostlen);                                \
	marshall_STRING(p, (rmnode)->stagerhost);                            \
	marshall_HYPER(p, (rmnode)->stagerport);                             \
	marshall_TIME_T(p, (rmnode)->update);                                \
	marshall_STRING(p, (rmnode)->state);                                 \
	marshall_STRING(p, (rmnode)->osname);                                \
	marshall_STRING(p, (rmnode)->arch);                                  \
	marshall_HYPER(p, (rmnode)->totalram);                               \
	marshall_HYPER(p, (rmnode)->freeram);                                \
	marshall_HYPER(p, (rmnode)->usedram);                                \
	marshall_HYPER(p, (rmnode)->totalmem);                               \
	marshall_HYPER(p, (rmnode)->usedmem);                                \
	marshall_HYPER(p, (rmnode)->freemem);                                \
	marshall_HYPER(p, (rmnode)->totalswap);                              \
	marshall_HYPER(p, (rmnode)->usedswap);                               \
	marshall_HYPER(p, (rmnode)->freeswap);                               \
	marshall_HYPER(p, (rmnode)->totalproc);                              \
	marshall_HYPER(p, (rmnode)->availproc);                              \
	marshall_HYPER(p, (rmnode)->nifce);                                  \
    if ((rmnode)->nifce > 0) {                                           \
		int i;                                                           \
	    struct Crm_ifconf *ifce  = (rmnode)->ifce;                       \
		for (i = 0; i < (rmnode)->nifce; ifce++, i++) {                  \
	  	  marshall_STRING(p, ifce->name);                                \
	  	  marshall_STRING(p, ifce->status);                              \
	    }                                                                \
	}                                                                    \
	marshall_HYPER(p, (rmnode)->load);                                   \
	marshall_STRING(p, (rmnode)->feature);                               \
	marshall_HYPER(p, (rmnode)->nfs);                                    \
    if ((rmnode)->nfs > 0) {                                             \
		int i;                                                           \
		for (i = 0; i < (rmnode)->nfs; i++) {                            \
		    marshall_STRING(p, (rmnode)->fs[i].name);                    \
		    marshall_STRING(p, (rmnode)->fs[i].partition);               \
			marshall_STRING(p, (rmnode)->fs[i].nickname);                \
		    marshall_HYPER(p,  (rmnode)->fs[i].totalspace);              \
		    marshall_HYPER(p,  (rmnode)->fs[i].freespace);               \
		    marshall_HYPER(p,  (rmnode)->fs[i].rd);                      \
		    marshall_HYPER(p,  (rmnode)->fs[i].wr);                      \
		    marshall_HYPER(p,  (rmnode)->fs[i].wr);                      \
		    marshall_HYPER(p,  (rmnode)->fs[i].r_rate);                  \
		    marshall_HYPER(p,  (rmnode)->fs[i].w_rate);                  \
		    marshall_HYPER(p,  (rmnode)->fs[i].tot_rate);                \
		    marshall_HYPER(p,  (rmnode)->fs[i].n_rdonly);                \
		    marshall_HYPER(p,  (rmnode)->fs[i].n_wronly);                \
		    marshall_HYPER(p,  (rmnode)->fs[i].n_rdwr);                  \
		    marshall_HYPER(p,  (rmnode)->fs[i].spacefrequency);          \
		    marshall_HYPER(p,  (rmnode)->fs[i].ioratefrequency)          \
		    marshall_LONG (p,  (rmnode)->fs[i].localaccess);             \
		    marshall_STRING(p, (rmnode)->fs[i].state);                   \
		    if (magic >= RMMAGIC03) {                                    \
	  		marshall_LONG(p, (rmnode)->fs[i].forced_state);          \
	  	    }                                                            \
	  	}                                                            \
	}                                                                    \
	marshall_HYPER(p, (rmnode)->n_rdonly);                               \
	marshall_HYPER(p, (rmnode)->n_wronly);                               \
	marshall_HYPER(p, (rmnode)->n_rdwr);                                 \
	marshall_HYPER(p, (rmnode)->maxtask);                                 \
	marshall_STRING(p, (rmnode)->partition);                              \
	marshall_STRING(p, (rmnode)->feature);                                \
}

#define unmarshall_RMNODE(p, rmnode) {                                     \
	unmarshall_STRING(p, (rmnode)->host);                                  \
	unmarshall_HYPER(p, (rmnode)->hostlen);                                \
	unmarshall_STRING(p, (rmnode)->stagerhost);                            \
	unmarshall_HYPER(p, (rmnode)->stagerport);                             \
	unmarshall_TIME_T(p, (rmnode)->update);                                \
	unmarshall_STRING(p, (rmnode)->state);                                 \
	unmarshall_STRING(p, (rmnode)->osname);                                \
	unmarshall_STRING(p, (rmnode)->arch);                                  \
	unmarshall_HYPER(p, (rmnode)->totalram);                               \
	unmarshall_HYPER(p, (rmnode)->freeram);                                \
	unmarshall_HYPER(p, (rmnode)->usedram);                                \
	unmarshall_HYPER(p, (rmnode)->totalmem);                               \
	unmarshall_HYPER(p, (rmnode)->usedmem);                                \
	unmarshall_HYPER(p, (rmnode)->freemem);                                \
	unmarshall_HYPER(p, (rmnode)->totalswap);                              \
	unmarshall_HYPER(p, (rmnode)->usedswap);                               \
	unmarshall_HYPER(p, (rmnode)->freeswap);                               \
	unmarshall_HYPER(p, (rmnode)->totalproc);                              \
	unmarshall_HYPER(p, (rmnode)->availproc);                              \
	unmarshall_HYPER(p, (rmnode)->nifce);                                  \
    if ((rmnode)->nifce > 0) {                                             \
	  if (((rmnode)->ifce = (struct Crm_ifconf *)                          \
		   malloc(((rmnode)->nifce * sizeof(struct Crm_ifconf)))) != NULL) { \
		  int i;                                                           \
		  struct Crm_ifconf *ifce  = (rmnode)->ifce;                       \
		  for (i = 0; i < (rmnode)->nifce; ifce++, i++) {                  \
			  unmarshall_STRING(p, ifce->name);                            \
			  unmarshall_STRING(p, ifce->status);                          \
		  }                                                                \
	  }                                                                    \
	}                                                                      \
	unmarshall_HYPER(p, (rmnode)->load);                                   \
	unmarshall_STRING(p, (rmnode)->feature);                               \
	unmarshall_HYPER(p, (rmnode)->nfs);                                    \
    if ((rmnode)->nfs > 0) {                                               \
	  if (((rmnode)->fs = (struct Crm_filesystem *)                        \
		   malloc(((rmnode)->nfs * sizeof(struct Crm_filesystem)))) != NULL) { \
		  int i;                                                           \
		  for (i = 0; i < (rmnode)->nfs; i++) {                            \
			  unmarshall_STRING(p, (rmnode)->fs[i].name);                  \
			  unmarshall_STRING(p, (rmnode)->fs[i].partition);             \
			  unmarshall_STRING(p, (rmnode)->fs[i].nickname);              \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].totalspace);            \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].freespace);             \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].rd);                    \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].wr);                    \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].wr);                    \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].r_rate);                \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].w_rate);                \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].tot_rate);              \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].n_rdonly);              \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].n_wronly);              \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].n_rdwr);                \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].spacefrequency);        \
			  unmarshall_HYPER(p,  (rmnode)->fs[i].ioratefrequency);       \
			  unmarshall_LONG(p,  (rmnode)->fs[i].localaccess);            \
			  unmarshall_STRING(p,(rmnode)->fs[i].state);                  \
		    	  if (magic >= RMMAGIC03) {                                    \
	  		  	 unmarshall_LONG(p, (rmnode)->fs[i].forced_state);     \
	  	    	  }                                                            \
		}                                                                  \
	  }                                                                    \
	}                                                                      \
	unmarshall_HYPER(p, (rmnode)->n_rdonly);                               \
	unmarshall_HYPER(p, (rmnode)->n_wronly);                               \
	unmarshall_HYPER(p, (rmnode)->n_rdwr);                                 \
	unmarshall_HYPER(p, (rmnode)->maxtask);                                 \
	unmarshall_STRING(p, (rmnode)->partition);                              \
	unmarshall_STRING(p, (rmnode)->feature);                                \
}

#define marshall_RMJOB(p, rmjob) {                      \
	marshall_STRING(p, (rmjob)->jobid);                 \
	marshall_TIME_T(p, (rmjob)->update);                \
	marshall_STRING(p, (rmjob)->state);                 \
	marshall_TIME_T(p, (rmjob)->wclimit);               \
	marshall_HYPER(p, (rmjob)->tasks);                  \
	marshall_HYPER(p, (rmjob)->nodes);                  \
	marshall_TIME_T(p, (rmjob)->queuetime);             \
	marshall_TIME_T(p, (rmjob)->startdate);             \
	marshall_TIME_T(p, (rmjob)->starttime);             \
	marshall_TIME_T(p, (rmjob)->completiontime);        \
	marshall_STRING(p, (rmjob)->uname);                 \
	marshall_HYPER(p, (rmjob)->uid);                    \
	marshall_STRING(p, (rmjob)->gname);                 \
	marshall_HYPER(p, (rmjob)->gid);                    \
	marshall_STRING(p, (rmjob)->account);               \
	marshall_STRING(p, (rmjob)->rfeatures);             \
	marshall_STRING(p, (rmjob)->rnetwork);              \
	marshall_STRING(p, (rmjob)->dnetwork);              \
	marshall_STRING(p, (rmjob)->ropsys);                \
	marshall_STRING(p, (rmjob)->rarch);                 \
	marshall_HYPER(p, (rmjob)->rmem);                   \
	marshall_STRING(p, (rmjob)->rmemcmp);               \
	marshall_HYPER(p, (rmjob)->dmem);                   \
	marshall_HYPER(p, (rmjob)->rdisk);                  \
	marshall_STRING(p, (rmjob)->rdiskcmp);              \
	marshall_HYPER(p, (rmjob)->ddisk);                  \
	marshall_HYPER(p, (rmjob)->rswap);                  \
	marshall_STRING(p, (rmjob)->rswapcmp);              \
	marshall_HYPER(p, (rmjob)->dswap);                  \
	marshall_HYPER(p, (rmjob)->qos);                    \
	marshall_TIME_T(p, (rmjob)->enddate);               \
	marshall_HYPER(p, (rmjob)->dprocs);                 \
	marshall_STRING(p, (rmjob)->reservation);           \
	marshall_STRING(p, (rmjob)->stageid);               \
	marshall_STRING(p, (rmjob)->tasklist);              \
	marshall_SHORT(p, (rmjob)->subreqid);               \
	marshall_STRING(p, (rmjob)->exec);                  \
	marshall_HYPER(p, (rmjob)->lsf_jobid);              \
	marshall_HYPER(p, (rmjob)->finished_job_scanned_by_maui); \
	marshall_LONG(p, (rmjob)->signal);                  \
	marshall_HYPER(p, (rmjob)->openflags);              \
	marshall_STRING(p, (rmjob)->fs);                    \
	marshall_HYPER(p, (rmjob)->processid);              \
	marshall_STRING(p, (rmjob)->partitionmask);         \
	marshall_STRING(p, (rmjob)->requestid);             \
	marshall_STRING(p, (rmjob)->subrequestid);          \
	if ((rmjob)->clientStruct != NULL) {                \
          (rmjob)->clientStructLenWithNullByte = strlen((rmjob)->clientStruct) + 1; \
        } else {                                            \
          (rmjob)->clientStructLenWithNullByte = 0;         \
        }                                                   \
        marshall_HYPER(p, (rmjob)->clientStructLenWithNullByte);  \
	if ((rmjob)->clientStructLenWithNullByte > 0) marshall_STRING(p, (rmjob)->clientStruct); \
	marshall_STRING(p, (rmjob)->hostlist);              \
	marshall_STRING(p, (rmjob)->rfs);                   \
	if (magic >= RMMAGIC02) {                           \
	  marshall_HYPER(p, (rmjob)->castorFileId);         \
	  marshall_STRING(p, (rmjob)->castorNsHost);        \
	}                                                   \
}

#define unmarshall_RMJOB(p, rmjob, status) {                  \
	unmarshall_STRING(p, (rmjob)->jobid);                 \
	unmarshall_TIME_T(p, (rmjob)->update);                \
	unmarshall_STRING(p, (rmjob)->state);                 \
	unmarshall_TIME_T(p, (rmjob)->wclimit);               \
	unmarshall_HYPER(p, (rmjob)->tasks);                  \
	unmarshall_HYPER(p, (rmjob)->nodes);                  \
	unmarshall_TIME_T(p, (rmjob)->queuetime);             \
	unmarshall_TIME_T(p, (rmjob)->startdate);             \
	unmarshall_TIME_T(p, (rmjob)->starttime);             \
	unmarshall_TIME_T(p, (rmjob)->completiontime);        \
	unmarshall_STRING(p, (rmjob)->uname);                 \
	unmarshall_HYPER(p, (rmjob)->uid);                    \
	unmarshall_STRING(p, (rmjob)->gname);                 \
	unmarshall_HYPER(p, (rmjob)->gid);                    \
	unmarshall_STRING(p, (rmjob)->account);               \
	unmarshall_STRING(p, (rmjob)->rfeatures);             \
	unmarshall_STRING(p, (rmjob)->rnetwork);              \
	unmarshall_STRING(p, (rmjob)->dnetwork);              \
	unmarshall_STRING(p, (rmjob)->ropsys);                \
	unmarshall_STRING(p, (rmjob)->rarch);                 \
	unmarshall_HYPER(p, (rmjob)->rmem);                   \
	unmarshall_STRING(p, (rmjob)->rmemcmp);               \
	unmarshall_HYPER(p, (rmjob)->dmem);                   \
	unmarshall_HYPER(p, (rmjob)->rdisk);                  \
	unmarshall_STRING(p, (rmjob)->rdiskcmp);              \
	unmarshall_HYPER(p, (rmjob)->ddisk);                  \
	unmarshall_HYPER(p, (rmjob)->rswap);                  \
	unmarshall_STRING(p, (rmjob)->rswapcmp);              \
	unmarshall_HYPER(p, (rmjob)->dswap);                  \
	unmarshall_HYPER(p, (rmjob)->qos);                    \
	unmarshall_TIME_T(p, (rmjob)->enddate);               \
	unmarshall_HYPER(p, (rmjob)->dprocs);                 \
	unmarshall_STRING(p, (rmjob)->reservation);           \
	unmarshall_STRING(p, (rmjob)->stageid);               \
	unmarshall_STRING(p, (rmjob)->tasklist);              \
	unmarshall_SHORT(p, (rmjob)->subreqid);               \
	unmarshall_STRING(p, (rmjob)->exec);                  \
	unmarshall_HYPER(p, (rmjob)->lsf_jobid);              \
	unmarshall_HYPER(p, (rmjob)->finished_job_scanned_by_maui); \
	unmarshall_LONG(p, (rmjob)->signal);                  \
	unmarshall_HYPER(p, (rmjob)->openflags);              \
	unmarshall_STRING(p, (rmjob)->fs);                    \
	unmarshall_HYPER(p, (rmjob)->processid);              \
	unmarshall_STRING(p, (rmjob)->partitionmask);         \
	unmarshall_STRING(p, (rmjob)->requestid);             \
	unmarshall_STRING(p, (rmjob)->subrequestid);          \
	unmarshall_HYPER(p, (rmjob)->clientStructLenWithNullByte); \
        if ((rmjob)->clientStructLenWithNullByte > 0) {       \
          if (((rmjob)->clientStruct = (char *) malloc((rmjob)->clientStructLenWithNullByte)) == NULL) { \
            serrno = errno;                                   \
            status = -1;                                      \
          } else {                                            \
            unmarshall_STRING(p, (rmjob)->clientStruct);      \
          }                                                   \
        } else {                                              \
          (rmjob)->clientStruct = NULL;                       \
        }                                                     \
	unmarshall_STRING(p, (rmjob)->hostlist);              \
	unmarshall_STRING(p, (rmjob)->rfs);                   \
	if (magic >= RMMAGIC02) {                             \
	  unmarshall_HYPER(p, (rmjob)->castorFileId);         \
	  unmarshall_STRING(p, (rmjob)->castorNsHost);        \
	}                                                     \
}

#define overwrite_RMJOB(out,in,status) {                                            \
    if ((in)->jobid[0] != '\0') strcpy((out)->jobid,(in)->jobid);                   \
    if ((in)->update > 0) (out)->update = (in)->update;                             \
    if ((in)->state[0] != '\0') strcpy((out)->state,(in)->state);                   \
    if ((in)->wclimit > 0) (out)->wclimit = (in)->wclimit;                          \
    if ((in)->tasks > 0) (out)->tasks = (in)->tasks;                                \
    if ((in)->nodes > 0) (out)->nodes = (in)->nodes;                                \
    if ((in)->queuetime > 0) (out)->queuetime = (in)->queuetime;                    \
    if ((in)->startdate > 0) (out)->startdate = (in)->startdate;                    \
    if ((in)->starttime > 0) (out)->starttime = (in)->starttime;                    \
    if ((in)->completiontime > 0) (out)->completiontime = (in)->completiontime;     \
    if ((in)->uname[0] != '\0') strcpy((out)->uname,(in)->uname);                   \
    if ((in)->uid > 0) (out)->uid = (in)->uid;                                      \
    if ((in)->gname[0] != '\0') strcpy((out)->gname,(in)->gname);                   \
    if ((in)->gid > 0) (out)->gid = (in)->gid;                                      \
    if ((in)->account[0] != '\0') strcpy((out)->account,(in)->account);             \
    if ((in)->rfeatures[0] != '\0') strcpy((out)->rfeatures,(in)->rfeatures);       \
    if ((in)->rnetwork[0] != '\0') strcpy((out)->rnetwork,(in)->rnetwork);          \
    if ((in)->dnetwork[0] != '\0') strcpy((out)->dnetwork,(in)->dnetwork);          \
    if ((in)->ropsys[0] != '\0') strcpy((out)->ropsys,(in)->ropsys);                \
    if ((in)->rarch[0] != '\0') strcpy((out)->rarch,(in)->rarch);                   \
    if ((in)->rmem > 0) (out)->rmem = (in)->rmem;                                   \
    if ((in)->rmemcmp[0] != '\0') strcpy((out)->rmemcmp,(in)->rmemcmp);             \
    if ((in)->dmem > 0) (out)->dmem = (in)->dmem;                                   \
    if ((in)->rdisk > 0) (out)->rdisk = (in)->rdisk;                                \
    if ((in)->rdiskcmp[0] != '\0') strcpy((out)->rdiskcmp,(in)->rdiskcmp);          \
    if ((in)->ddisk > 0) (out)->ddisk = (in)->ddisk;                                \
    if ((in)->rswap > 0) (out)->rswap = (in)->rswap;                                \
    if ((in)->rswapcmp[0] != '\0') strcpy((out)->rswapcmp,(in)->rswapcmp);          \
    if ((in)->dswap > 0) (out)->dswap = (in)->dswap;                                \
    if ((in)->qos > 0) (out)->qos = (in)->qos;                                      \
    if ((in)->enddate > 0) (out)->enddate = (in)->enddate;                          \
    if ((in)->dprocs != 1) (out)->dprocs = (in)->dprocs;                            \
    if ((in)->reservation[0] != '\0') strcpy((out)->reservation,(in)->reservation); \
    if ((in)->stageid[0] != '\0') strcpy((out)->stageid,(in)->stageid);             \
    if ((in)->tasklist[0] != '\0') strcpy((out)->tasklist,(in)->tasklist);          \
    if ((in)->subreqid > 0) (out)->subreqid = (in)->subreqid;                       \
    if ((in)->exec[0] != '\0') strcpy((out)->exec,(in)->exec);                      \
    if ((in)->lsf_jobid > 0) (out)->lsf_jobid = (in)->lsf_jobid;                    \
    if ((in)->finished_job_scanned_by_maui > 0) (out)->finished_job_scanned_by_maui = (in)->finished_job_scanned_by_maui; \
    if ((in)->signal > 0) (out)->signal = (in)->signal;                             \
    if ((in)->openflags > 0) (out)->openflags = (in)->openflags;                    \
    if ((in)->fs[0] != '\0') strcpy((out)->fs,(in)->fs);                            \
    if ((in)->processid > 0) (out)->processid = (in)->processid;                    \
    if ((in)->partitionmask[0] != '\0') strcpy((out)->partitionmask,(in)->partitionmask); \
    if ((in)->requestid[0] != '\0') strcpy((out)->requestid,(in)->requestid);       \
    if ((in)->subrequestid[0] != '\0') strcpy((out)->subrequestid,(in)->subrequestid); \
    if ((in)->clientStruct != NULL && (out)->clientStruct != NULL) { free((out)->clientStruct); (out)->clientStruct = NULL; } \
    if ((in)->clientStructLenWithNullByte > 0) (out)->clientStructLenWithNullByte = (in)->clientStructLenWithNullByte; \
    if ((in)->clientStructLenWithNullByte > 0) {                                    \
          if (((out)->clientStruct = (char *) malloc((out)->clientStructLenWithNullByte)) == NULL) { \
            serrno = errno;                                   \
            status = -1;                                      \
          } else {                                            \
            strcpy((out)->clientStruct,(in)->clientStruct);   \
          }                                                   \
    }                                                         \
    if ((in)->hostlist[0] != '\0') strcpy((out)->hostlist,(in)->hostlist);          \
    if ((in)->rfs[0] != '\0') strcpy((out)->rfs,(in)->rfs);                         \
    if ((in)->castorFileId != 0) (out)->castorFileId = (in)->castorFileId;             \
    if ((in)->castorNsHost[0] != '\0') strcpy((out)->castorNsHost,(in)->castorNsHost); \
}

#define cmp_RMJOB(status,filter,ref) {                                                                        \
    if ((filter)->jobid[0] != '\0' && strcmp((ref)->jobid,(filter)->jobid) != 0)                   status++;  \
    if ((filter)->update > 0 && (ref)->update != (filter)->update)                                 status++;  \
    if ((filter)->state[0] != '\0' && strcmp((ref)->state,(filter)->state) != 0)                   status++;  \
    if ((filter)->wclimit > 0 && (ref)->wclimit != (filter)->wclimit)                              status++;  \
    if ((filter)->tasks > 0 && (ref)->tasks != (filter)->tasks)                                    status++;  \
    if ((filter)->nodes > 0 && (ref)->nodes != (filter)->nodes)                                    status++;  \
    if ((filter)->queuetime > 0 && (ref)->queuetime != (filter)->queuetime)                        status++;  \
    if ((filter)->startdate > 0 && (ref)->startdate != (filter)->startdate)                        status++;  \
    if ((filter)->starttime > 0 && (ref)->starttime != (filter)->starttime)                        status++;  \
    if ((filter)->completiontime > 0 && (ref)->completiontime != (filter)->completiontime)         status++;  \
    if ((filter)->uname[0] != '\0' && strcmp((ref)->uname,(filter)->uname) != 0)                   status++;  \
    if ((filter)->uid > 0 && (ref)->uid != (filter)->uid)                                          status++;  \
    if ((filter)->gname[0] != '\0' && strcmp((ref)->gname,(filter)->gname) != 0)                   status++;  \
    if ((filter)->gid > 0 && (ref)->gid != (filter)->gid)                                          status++;  \
    if ((filter)->account[0] != '\0' && strcmp((ref)->account,(filter)->account) != 0)             status++;  \
    if ((filter)->rfeatures[0] != '\0' && strcmp((ref)->rfeatures,(filter)->rfeatures) != 0)       status++;  \
    if ((filter)->rnetwork[0] != '\0' && strcmp((ref)->rnetwork,(filter)->rnetwork) != 0)          status++;  \
    if ((filter)->dnetwork[0] != '\0' && strcmp((ref)->dnetwork,(filter)->dnetwork) != 0)          status++;  \
    if ((filter)->ropsys[0] != '\0' && strcmp((ref)->ropsys,(filter)->ropsys) != 0)                status++;  \
    if ((filter)->rarch[0] != '\0' && strcmp((ref)->rarch,(filter)->rarch) != 0)                   status++;  \
    if ((filter)->rmem > 0 && (ref)->rmem != (filter)->rmem)                                       status++;  \
    if ((filter)->rmemcmp[0] != '\0' && strcmp((ref)->rmemcmp,(filter)->rmemcmp) != 0)             status++;  \
    if ((filter)->dmem > 0 && (ref)->dmem != (filter)->dmem)                                       status++;  \
    if ((filter)->rdisk > 0 && (ref)->rdisk != (filter)->rdisk)                                    status++;  \
    if ((filter)->rdiskcmp[0] != '\0' && strcmp((ref)->rdiskcmp,(filter)->rdiskcmp) != 0)          status++;  \
    if ((filter)->ddisk > 0 && (ref)->ddisk != (filter)->ddisk)                                    status++;  \
    if ((filter)->rswap > 0 && (ref)->rswap != (filter)->rswap)                                    status++;  \
    if ((filter)->rswapcmp[0] != '\0' && strcmp((ref)->rswapcmp,(filter)->rswapcmp) != 0)          status++;  \
    if ((filter)->dswap > 0 && (ref)->dswap != (filter)->dswap)                                    status++;  \
    if ((filter)->qos > 0 && (ref)->qos != (filter)->qos)                                          status++;  \
    if ((filter)->enddate > 0 && (ref)->enddate != (filter)->enddate)                              status++;  \
    if ((filter)->dprocs != 1 && (ref)->dprocs != (filter)->dprocs)                                status++; \
    if ((filter)->reservation[0] != '\0' && strcmp((ref)->reservation,(filter)->reservation) != 0) status++;  \
    if ((filter)->stageid[0] != '\0' && strcmp((ref)->stageid,(filter)->stageid) != 0)             status++;  \
    if ((filter)->tasklist[0] != '\0' && strcmp((ref)->tasklist,(filter)->tasklist) != 0)          status++;  \
    if ((filter)->subreqid > 0 && (ref)->subreqid != (filter)->subreqid)                           status++;  \
    if ((filter)->exec[0] != '\0' && strcmp((ref)->exec,(filter)->exec) != 0)                      status++;  \
    if ((filter)->lsf_jobid > 0 && (ref)->lsf_jobid != (filter)->lsf_jobid)                        status++;  \
    if ((filter)->finished_job_scanned_by_maui > 0 && (ref)->finished_job_scanned_by_maui != (filter)->finished_job_scanned_by_maui) status++;  \
    if ((filter)->signal > 0 && (ref)->signal != (filter)->signal)                                 status++;  \
    if ((filter)->openflags > 0 && (ref)->openflags != (filter)->openflags)                        status++;  \
    if ((filter)->fs[0] != '\0' && strcmp((ref)->fs,(filter)->fs) != 0)                            status++;  \
    if ((filter)->processid > 0 && (ref)->processid != (filter)->processid)                        status++;  \
    if ((filter)->partitionmask[0] != '\0' && strcmp((ref)->partitionmask,(filter)->partitionmask) != 0) status++;  \
    if ((filter)->requestid[0] != '\0' && strcmp((ref)->requestid,(filter)->requestid) != 0) status++;  \
    if ((filter)->subrequestid[0] != '\0' && strcmp((ref)->subrequestid,(filter)->subrequestid) != 0) status++;  \
    if ((filter)->clientStructLenWithNullByte > 0 && (ref)->clientStructLenWithNullByte != (filter)->clientStructLenWithNullByte) status++;  \
    if ((filter)->clientStruct != NULL && memcmp((ref)->clientStruct, (filter)->clientStruct, (filter)->clientStructLenWithNullByte) != 0) status++;  \
    if ((filter)->hostlist[0] != '\0' && strcmp((ref)->hostlist,(filter)->hostlist) != 0)          status++;  \
    if ((filter)->rfs[0] != '\0' && strcmp((ref)->rfs,(filter)->rfs) != 0)                         status++;  \
    if ((filter)->castorFileId != 0 && (ref)->castorFileId != (filter)->castorFileId) status++;               \
    if ((filter)->castorNsHost[0] != '\0' && strcmp((ref)->castorNsHost,(filter)->castorNsHost) != 0) status++;  \
}

#define unmarshall_RELEVANT_RMJOB(p, out, status) {  \
    struct rmjob rmjobtmp;                    \
    unmarshall_RMJOB(p, &rmjobtmp,status);    \
    if (status == 0) {                        \
      overwrite_RMJOB(out,&rmjobtmp,status);  \
    }                                         \
}

#define unmarshall_RELEVANT_RMNODE(p, out) {   \
    struct rmnode rmnodetmp;                   \
    unmarshall_RMNODE(p, &rmnodetmp);          \
    overwrite_RMNODE(out,&rmnodetmp);          \
}

/* NOTE: IFCE and FS are not supported in this method */
#define overwrite_RMNODE(out,in) {                                                   \
    if ((in)->host[0] != '\0') strcpy((out)->host,(in)->host);                       \
    if ((in)->hostlen > 0) (out)->hostlen = (in)->hostlen;                           \
    if ((in)->stagerhost[0] != '\0') strcpy((out)->stagerhost,(in)->stagerhost);     \
    if ((in)->stagerport > 0) (out)->stagerport = (in)->stagerport;                  \
    if ((in)->update > 0) (out)->update = (in)->update;                              \
    if ((in)->state[0] != '\0') strcpy((out)->state,(in)->state);                    \
    if ((in)->osname[0] != '\0') strcpy((out)->osname,(in)->osname);                 \
    if ((in)->arch[0] != '\0') strcpy((out)->arch,(in)->arch);                       \
    if ((in)->totalram > 0) (out)->totalram = (in)->totalram;                        \
    if ((in)->freeram > 0) (out)->freeram = (in)->freeram;                           \
    if ((in)->usedram > 0) (out)->usedram = (in)->usedram;                           \
    if ((in)->totalmem > 0) (out)->totalmem = (in)->totalmem;                        \
    if ((in)->freemem > 0) (out)->freemem = (in)->freemem;                           \
    if ((in)->usedmem > 0) (out)->usedmem = (in)->usedmem;                           \
    if ((in)->totalswap > 0) (out)->totalswap = (in)->totalswap;                     \
    if ((in)->freeswap > 0) (out)->freeswap = (in)->freeswap;                        \
    if ((in)->usedswap > 0) (out)->usedswap = (in)->usedswap;                        \
    if ((in)->totalproc > 0) (out)->totalproc = (in)->totalproc;                     \
    if ((in)->availproc > 0) (out)->availproc = (in)->availproc;                     \
    if ((in)->load > 0) (out)->load = (in)->load;                                    \
    if ((in)->feature[0] != '\0') strcpy((out)->feature,(in)->feature);              \
    if ((in)->n_rdonly > 0) (out)->n_rdonly = (in)->n_rdonly;                        \
    if ((in)->n_wronly > 0) (out)->n_wronly = (in)->n_wronly;                        \
    if ((in)->n_rdwr > 0) (out)->n_rdwr = (in)->n_rdwr;                              \
    if ((in)->maxtask > 0) (out)->maxtask = (in)->maxtask;                           \
    if ((in)->partition[0] != '\0') strcpy((out)->partition,(in)->partition);        \
    if ((in)->feature[0] != '\0') strcpy((out)->feature,(in)->feature);        \
}

/* NOTE: IFCE and FS are not supported in this method */
#define cmp_RMNODE(status,filter,ref) {                                                                    \
    if ((filter)->host[0] != '\0' && strcmp((ref)->host,(filter)->host) != 0)                   status++;  \
    if ((filter)->stagerhost[0] != '\0' && strcmp((ref)->stagerhost,(filter)->stagerhost) != 0) status++;  \
    if ((filter)->stagerport > 0 && (ref)->stagerport != (filter)->stagerport)                  status++;  \
    if ((filter)->update > 0 && (ref)->update != (filter)->update)                              status++;  \
    if ((filter)->state[0] != '\0' && strcmp((ref)->state,(filter)->state) != 0)                status++;  \
    if ((filter)->osname[0] != '\0' && strcmp((ref)->osname,(filter)->osname) != 0)             status++;  \
    if ((filter)->arch[0] != '\0' && strcmp((ref)->arch,(filter)->arch) != 0)                   status++;  \
    if ((filter)->totalram > 0 && (ref)->totalram != (filter)->totalram)                        status++;  \
    if ((filter)->freeram > 0 && (ref)->freeram != (filter)->freeram)                           status++;  \
    if ((filter)->usedram > 0 && (ref)->usedram != (filter)->usedram)                           status++;  \
    if ((filter)->totalmem > 0 && (ref)->totalmem != (filter)->totalmem)                        status++;  \
    if ((filter)->freemem > 0 && (ref)->freemem != (filter)->freemem)                           status++;  \
    if ((filter)->usedmem > 0 && (ref)->usedmem != (filter)->usedmem)                           status++;  \
    if ((filter)->totalswap > 0 && (ref)->totalswap != (filter)->totalswap)                     status++;  \
    if ((filter)->freeswap > 0 && (ref)->freeswap != (filter)->freeswap)                        status++;  \
    if ((filter)->usedswap > 0 && (ref)->usedswap != (filter)->usedswap)                        status++;  \
    if ((filter)->totalproc > 0 && (ref)->totalproc != (filter)->totalproc)                     status++;  \
    if ((filter)->availproc > 0 && (ref)->availproc != (filter)->availproc)                     status++;  \
    if ((filter)->load > 0 && (ref)->load != (filter)->load)                                    status++;  \
    if ((filter)->feature[0] != '\0' && strcmp((ref)->feature,(filter)->feature) != 0)          status++;  \
    if ((filter)->n_rdonly > 0 && (ref)->n_rdonly != (filter)->n_rdonly)                        status++;  \
    if ((filter)->n_wronly > 0 && (ref)->n_wronly != (filter)->n_wronly)                        status++;  \
    if ((filter)->n_rdwr > 0 && (ref)->n_rdwr != (filter)->n_rdwr)                              status++;  \
    if ((filter)->maxtask > 0 && (ref)->maxtask != (filter)->maxtask)                           status++;  \
    if ((filter)->partition[0] != '\0' && strcmp((ref)->partition,(filter)->partition) != 0)    status++;  \
    if ((filter)->feature[0] != '\0' && strcmp((ref)->feature,(filter)->feature) != 0)          status++;  \
}

#define RM_ENTER() {                              \
	strcpy(funcrep_name,func);                    \
	funcrep_name[MAX_FUNCREP_NAME_LENGTH] = '\0'; \
	log(LOG_INFO, "%s: Entering\n", func);        \
	if (! nodlf_flag) {                          \
	    int _save_serrno = serrno;               \
	    dlf_write(                               \
	   	     rmmaster_uuid,                  \
	     	     DLF_LVL_DEBUG,                  \
	     	     RM_DLF_MSG_ENTERING,      \
		     (struct Cns_fileid *) NULL,     \
		     1,                              \
		     "FUNC",DLF_MSG_PARAM_STR,func); \
	   serrno = _save_serrno;                    \
       }                                             \
}

#define RM_LEAVE() {                              \
	log(LOG_INFO, "%s: Leaving\n", func);         \
	if (! nodlf_flag) {                          \
	    int _save_serrno = serrno;               \
	    dlf_write(                               \
	   	     rmmaster_uuid,                  \
	     	     DLF_LVL_DEBUG,                  \
	     	     RM_DLF_MSG_LEAVING,      \
		     (struct Cns_fileid *) NULL,     \
		     1,                              \
		     "FUNC",DLF_MSG_PARAM_STR,func); \
	   serrno = _save_serrno;                    \
       }                                             \
}

#define RM_RETURN(value) {                        \
	RM_LEAVE();                                   \
	return(value);                                \
}

#define RM_EXIT(value) {                        \
	RM_LEAVE();                                   \
	exit(value);                                \
}

#endif /* __rm_macros_h */
