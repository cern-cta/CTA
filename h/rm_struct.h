#ifndef __rm_struct_h
#define __rm_struct_h

#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <sys/times.h>
#include <netdb.h>
#include <arpa/inet.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#endif
#include "osdep.h"
#include "Castor_limits.h"
#include "rm_constants.h"

struct rmjob {
	char client_host[CA_MAXHOSTNAMELEN+1];
	char jobid[RM_JOBIDLEN+1]; /* Incremental job id : make sure there is enough room to fit in a Cuuid */
	u_signed64 update; /* time job was last updated */
	char state[CA_MAXSTATUSLEN+1]; /* state of job ("Idle", "Running", "Hold", "Suspended", "Completed" or "Removed") */
	u_signed64 wclimit; /* seconds of wall time required by job */
	u_signed64 tasks; /* number of tasks required by job */
	u_signed64 nodes; /* number of nodes required by job */
	u_signed64 queuetime; /* time job was submitted to resource manager */
	u_signed64 startdate; /* earliest time job would be allowed to start */
	u_signed64 starttime; /* time job was started by the resource manager */
	u_signed64 completiontime; /* time job completed execution */
	char uname[RM_MAXUSRNAMELEN+1]; /* userid under which job will run */
	u_signed64 uid;      /* corresponding uid */
	char gname[RM_MAXGRPNAMELEN+1]; /* groupid under which job will run */
	u_signed64 gid;      /* corresponding gid */
	char account[RM_MAXACCOUNTNAMELEN+1]; /* accountid under which job will run */
	char rfeatures[RM_MAXFEATURELEN+1]; /* list of features required on node */
	char rnetwork[CRM_IFCONF_MAXNAMELEN+1]; /* network adapter required by job */
	char dnetwork[CRM_IFCONF_MAXNAMELEN+1]; /* network adapter which must be dedicated to job */
	char ropsys[1024]; /* operating system required by job */
	char rarch[1024]; /* architecture required by job */
	u_signed64 rmem;
	char rmemcmp[3]; /* ">=", ">", "==", "<" or "<=" */
	u_signed64 dmem;
	u_signed64 rdisk;
	char rdiskcmp[3]; /* ">=", ">", "==", "<" or "<=" */
	u_signed64 ddisk;
	u_signed64 rswap;
	char rswapcmp[3]; /* ">=", ">", "==", "<" or "<=" */
	u_signed64 dswap;
	u_signed64 rejcount; /* number of times job was rejected */
	char rejmessage[1024]; /* reason job was rejected */
	u_signed64 rejcode; /* reason job was rejected */
	u_signed64 qos; /* quality of service requested */
	u_signed64 enddate;	/* time by which job must complete */
	u_signed64 dprocs; /* number of processors dedicated per task */
	char reservation[1024]; /* name of reservation in which job must run */
	char stageid[1024]; /* for callback to stager - not used directly by maui */
	short subreqid; /* for callback to stager - not used directly by maui */
	char tasklist[CA_MAXHOSTNAMELEN+1];
	char exec[1024]; /* name of reservation in which job must run */
	u_signed64 lsf_jobid;
	u_signed64 finished_job_scanned_by_maui; /* Will be setted only if maui scanned it and job is 'Completed' or 'Removed' */
	int signal; /* In case it was signalled : value of the signal */
	int rm_child_mode; /* Internal use */
	int openflags; /* Hint to say O_RDONLY, O_WRONLY, etc... Any value >= 0 is meaningful - default is -1 */
	char fs[CA_MAXPATHLEN+1]; /* name of wished filesystem in which job must run */
	u_signed64 processid; /* processid, the one receiving a signal if the job is killed */
	char partitionmask[RM_MAXPARTITIONLEN+1]; /* partition mask */
	char requestid[CUUID_STRING_LEN+1];
	char subrequestid[CUUID_STRING_LEN+1];
	size_t clientStructLenWithNullByte;
	char *clientStruct;
	struct rmjob *next;
	struct rmjob *prev;
};

struct rmnode {
	char host[CA_MAXHOSTNAMELEN+1]; /* hostname after resolv */
	size_t hostlen; /* For optim */

	char stagerhost[CA_MAXHOSTNAMELEN+1]; /* stager hostname */
	int stagerport; /* stager port number */
	u_signed64 update; /* time node information was last updated */
	int forced_state; /* Boolean saying if status is forced from rmnode or not */
	char state[CA_MAXSTATUSLEN+1];  /* state of the node ("Auto", "Idle", "Running", "Busy", "Unknown", "Draining" or "Down") */
	char osname[CA_MAXOSNAMELEN+1]; /* operating system running on node */
	char arch[CA_MAXARCHNAMELEN+1]; /* compute architecture of node */
	u_signed64 totalram; /* configured ram on node (in MB) */
	u_signed64 freeram;  /* available/free ram on node (in MB) */
	u_signed64 usedram; /* used ram on node (in MB) */
	u_signed64 totalmem; /* configured memory on node (in MB) == totalram */
	u_signed64 usedmem; /* used memory taking into account buffers and cache (in MB) */
	u_signed64 freemem;  /* free memory taking into account buffers and cache (in MB) */
	u_signed64 totalswap; /* configured swap on node (in MB) */
	u_signed64 freeswap; /* available/free swap on node (in MB) */
	u_signed64 usedswap; /* used swap on node (in MB) */
	/* u_signed64 cdisk; */ /* c.f. Crmd_processmaui_getnodes */
	/* u_signed64 adisk; */ /* c.f. Crmd_processmaui_getnodes */
	int totalproc; /* configured processors on node */
	int availproc; /* available processors on node */
	int nifce;     /* number of network interfaces */
	struct Crm_ifconf *ifce; /* network interfaces with their status */
	int load; /* one minute BSD load average times 100 */
	char feature[RM_MAXFEATURELEN+1]; /* generic attributes */
	char partition[RM_MAXPARTITIONLEN+1]; /* partition */
	int nfs; /* number of filesystems */
	struct Crm_filesystem *fs; /* filesystems's totalspace,freespace */

	in_addr_t s_addr; /* Used to not rely on hostname persistence */
	int nbrestart;    /* To detect if rmnode was restarted */
	int n_rdonly;     /* Number of O_RDONLY jobs */
	int n_wronly;     /* Number of O_WRONLY jobs */
	int n_rdwr;       /* Number of non O_RDONLY or O_WRONLY jobs */
	u_signed64 maxtask; /* Maximum number of tasks */
	u_signed64 currenttask; /* Current number of running tasks */
};

struct Crm_ifconf {
	char name[CRM_IFCONF_MAXNAMELEN+1];
	char status[5]; /* "up" or "down" */
	struct Crm_ifconf *next; /* Used internally ONLY when collecting information on the local node */
};

struct Crm_filesystem {
	int index;
	char host[CA_MAXHOSTNAMELEN+1]; /* Not used, but can be useful for you if you want to qsort() fses from different machines */
	char name[CA_MAXPATHLEN+1];
	char partition[CA_MAXPATHLEN+1];
	int localaccess;
	char nickname[CA_MAXPATHLEN+1];
	u_signed64 totalspace;
	u_signed64 freespace;
	u_signed64 rd;
	u_signed64 wr;
	u_signed64 r_rate;
	u_signed64 w_rate;
	u_signed64 tot_rate;
	int n_rdonly;     /* Number of O_RDONLY jobs */
	int n_wronly;     /* Number of O_WRONLY jobs */
	int n_rdwr;       /* Number of non O_RDONLY or O_WRONLY jobs */
	int spacefrequency;
	int ioratefrequency;
	struct Crm_filesystem *next;
	char state[CA_MAXFSSTATUSLEN+1];  /* state of the filesystem ("Available", "Disabled" or "Draining") */
	u_signed64 reservedspace;
};

#endif /* __rm_struct_h */
