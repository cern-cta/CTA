/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)vdqm_AdmOps.c,v 1.1 2000/02/29 07:51:10  CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_AdmOps.c - Special routines for administrative operations.
 */


#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <Cpwd.h>
#include <time.h>
#include <osdep.h>
#include <net.h>
#include <log.h>
#include <serrno.h>
#include <Castor_limits.h>
#include <vdqm_constants.h>
#include <vdqm.h>

int vdqm_SetDedicate(vdqm_drvrec_t *drv) {
    vdqmDrvReq_t *DrvReq;
	char *p, *q, *r;
	
    if ( drv == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    DrvReq = &drv->drv;
#if DEBUG
    log(LOG_DEBUG,"vdqm_SetDedicate() compile regexp %s\n",DrvReq->dedicate);
#endif
    drv->expbuf = Cregexp_comp(DrvReq->dedicate);
    if ( drv->expbuf == NULL ) {
        log(LOG_ERR,"vdqm_SetDedicate() Cregexp_comp(%s): %s\n", DrvReq->dedicate,sstrerror(serrno));
        return(-1); 
	}

	memset(DrvReq->newdedicate,'\0',CA_MAXLINELEN+1);
	p = DrvReq->dedicate;
	
	/* Try to optimize the regexp */
	/* We expect it to be like this: "uid=...,gid=...,name=...,host=...,vid=...,mode=...,datestr=...,timestr=...,age=..." */
	DrvReq->is_uid = DrvReq->is_gid = DrvReq->is_name = DrvReq->no_uid = DrvReq->no_gid = DrvReq->no_name = DrvReq->no_host = DrvReq->no_vid = DrvReq->no_mode = DrvReq->no_date = DrvReq->no_time = DrvReq->no_age = 0;
	/* uid= */
	if ((q = strstr(p,"uid=")) == p) {
		r = q;
		p += strlen("uid=");
		if (*p != '\0') {
			if ((q = strstr(p,",gid=")) != NULL) {
				*q = '\0';
				/* Only digits for uid ? */
				if (strcmp(p,".*") == 0) {
					DrvReq->no_uid = 1;
#if DEBUG
					log(LOG_DEBUG,"vdqm_SetDedicate() : will skip uid in regexp\n");
#endif
				} else if (strspn(p,"0123456789") == strlen(p)) {
					/* Yes */
					DrvReq->is_uid = 1;
					DrvReq->uid = (uid_t) atoi(p);
					log(LOG_DEBUG,"vdqm_SetDedicate() : will use binary match on uid=%d\n",(int) DrvReq->uid);
					if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
					strcat(DrvReq->newdedicate,r);
				} else {
					if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
					strcat(DrvReq->newdedicate,r);
				}
				*q = ',';
				/* gid= */
				p = q + strlen(",gid=");
				if (*p != '\0') {
					r = q+1;
					if ((q = strstr(p,",name=")) != NULL) {
						*q = '\0';
						/* Only digits for gid ? */
						if (strcmp(p,".*") == 0) {
							DrvReq->no_gid = 1;
#if DEBUG
							log(LOG_DEBUG,"vdqm_SetDedicate() : will skip gid in regexp\n");
#endif
						} else if (strspn(p,"0123456789") == strlen(p)) {
							/* Yes */
							DrvReq->is_gid = 1;
							DrvReq->gid = (gid_t) atoi(p);
							log(LOG_DEBUG,"vdqm_SetDedicate() : will use binary match on gid=%d\n",(int) DrvReq->gid);
							if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
							strcat(DrvReq->newdedicate,r);
						} else {
							if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
							strcat(DrvReq->newdedicate,r);
						}
						*q = ',';
						/* name= */
						p = q + strlen(",name=");
						if (*p != '\0') {
							r = q+1;
							if ((q = strstr(p,",host=")) != NULL) {
								*q = '\0';
								/* Is it an already fully qualified name ? */
								if (strcmp(p,".*") == 0) {
									DrvReq->no_name = 1;
#if DEBUG
									log(LOG_DEBUG,"vdqm_SetDedicate() : will skip name in regexp\n");
#endif
								} else if (Cgetpwnam(p) != NULL) {
									/* Yes */
									if (strlen(p) <= CA_MAXUSRNAMELEN) {
										DrvReq->is_name = 1;
										strcpy(DrvReq->name,p);
										log(LOG_DEBUG,"vdqm_SetDedicate() : will use string match on name=%s\n",DrvReq->name);
									}
									if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
									strcat(DrvReq->newdedicate,r);
								} else {
									if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
									strcat(DrvReq->newdedicate,r);
								}
								*q = ',';
								/* For host, vid, mode, date, time, age: */
								/* There is no easy way to predict that data is valid or not */
								/* but at least we can say if there is such relevant or not !! */
								p = q + strlen(",host=");
								if (*p != '\0') {
									r = q+1;
									if ((q = strstr(p,".*")) == p) {
										/* The input says: 'host=.*' */
										DrvReq->no_host=1;
#if DEBUG
										log(LOG_DEBUG,"vdqm_SetDedicate() : will skip host in regexp\n");
#endif
										p = q + strlen(".*");
									} else {
										if ((p = strstr(p,",vid=")) == NULL) {
											log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",vid=\" string\n");
											goto no_optim;
										}
										*p = '\0';
										if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
										strcat(DrvReq->newdedicate,r);
										*p = ',';
									}
									if (*p == '\0') {
										log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",vid=\" string\n");
										goto no_optim;
									}
									
									r = p+1;
									if ((q = strstr(p,",vid=.*")) == p) {
										/* The input says: 'vid=.*' */
										DrvReq->no_vid=1;
#if DEBUG
										log(LOG_DEBUG,"vdqm_SetDedicate() : will skip vid in regexp\n");
#endif
										p = q + strlen(",vid=.*");
									} else {
										if ((p = strstr(p,",mode=")) == NULL) {
											log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",mode=\" string\n");
											goto no_optim;
										}
										*p = '\0';
										if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
										strcat(DrvReq->newdedicate,r);
										*p = ',';
									}
									if (*p == '\0') {
										log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",mode=\" string\n");
										goto no_optim;
									}
									
									r = p+1;
									if ((q = strstr(p,",mode=.*")) == p) {
										/* The input says: 'mode=.*' */
										DrvReq->no_mode=1;
#if DEBUG
										log(LOG_DEBUG,"vdqm_SetDedicate() : will skip mode in regexp\n");
#endif
										p = q + strlen(",mode=.*");
									} else {
										if ((p = strstr(p,",datestr=")) == NULL) {
											log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",datestr=\" string\n");
											goto no_optim;
										}
										*p = '\0';
										if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
										strcat(DrvReq->newdedicate,r);
										*p = ',';
									}
									if (*p == '\0') {
										log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",datestr=\" string\n");
										goto no_optim;
									}
									
									r = p+1;
									if ((q = strstr(p,",datestr=.*")) == p) {
										/* The input says: 'datestr=.*' */
										DrvReq->no_date=1;
#if DEBUG
										log(LOG_DEBUG,"vdqm_SetDedicate() : will skip date in regexp\n");
#endif
										p = q + strlen(",datestr=.*");
									} else {
										if ((p = strstr(p,",timestr=")) == NULL) {
											log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",timestr=\" string\n");
											goto no_optim;
										}
										*p = '\0';
										if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
										strcat(DrvReq->newdedicate,r);
										*p = ',';
									}
									if (*p == '\0') {
										log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",timestr=\" string\n");
										goto no_optim;
									}
									
									r = p+1;
									if ((q = strstr(p,",timestr=.*")) == p) {
										/* The input says: 'timestr=.*' */
										DrvReq->no_time=1;
#if DEBUG
										log(LOG_DEBUG,"vdqm_SetDedicate() : will skip time in regexp\n");
#endif
										p = q + strlen(",timestr=.*");
									} else {
										if ((p = strstr(p,",age=")) == NULL) {
											log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",age=\" string\n");
											goto no_optim;
										}
										*p = '\0';
										if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
										strcat(DrvReq->newdedicate,r);
										*p = ',';
									}
									if (*p == '\0') {
										log(LOG_DEBUG,"vdqm_SetDedicate() : missing \",age=\" string\n");
										goto no_optim;
									}
									
									r = p+1;
									if ((q = strstr(p,",age=.*")) == p) {
										/* The input says: 'age=.*' */
										DrvReq->no_age=1;
#if DEBUG
										log(LOG_DEBUG,"vdqm_SetDedicate() : will skip age in regexp\n");
#endif
									} else {
										*p = '\0';
										if (DrvReq->newdedicate[0] != '\0') strcat(DrvReq->newdedicate,",");
										strcat(DrvReq->newdedicate,r);
										*p = ',';
									}
								} else {
									goto no_optim;
								}
							} else {
								goto no_optim;
							}
						} else {
							goto no_optim;
						}
					} else {
						goto no_optim;
					}
				} else {
					goto no_optim;
				}
			} else {
				goto no_optim;
			}
		} else {
			goto no_optim;
		}
	} else {
		goto no_optim;
	}
	if (strcmp(DrvReq->newdedicate,DrvReq->dedicate) != 0) {
		/* Reset dedication */
		if (DrvReq->newdedicate[0] == '\0') {
			/* reduced to empty string !? */
			log(LOG_DEBUG,"vdqm_SetDedicate() : regexp reduced to \"\" (!?) - Will use full regexp\n");
			goto no_optim;
		} else {
			log(LOG_DEBUG,"vdqm_SetDedicate() : regexp reduced to \"%s\"\n", DrvReq->newdedicate);
			/* Evaluate new regexp */
#if DEBUG
			log(LOG_DEBUG,"vdqm_SetDedicate() compile regexp %s\n",DrvReq->newdedicate);
#endif
			drv->newexpbuf = Cregexp_comp(DrvReq->newdedicate);
			if ( drv->newexpbuf == NULL ) {
				log(LOG_ERR,"vdqm_SetDedicate() Cregexp_comp(%s): %s - Will use full regexp\n", DrvReq->newdedicate,sstrerror(serrno));
				goto no_optim;
			}
		}
	}
#if DEBUG
	log(LOG_DEBUG,"vdqm_SetDedicate() is_uid=%d, uid=%d, is_gid=%d, gid=%d, is_name=%d, name=%s\n", DrvReq->is_uid, DrvReq->uid, DrvReq->is_gid, DrvReq->gid, DrvReq->is_name, DrvReq->name);
#endif
    return(0);
  no_optim:
	DrvReq->is_uid = DrvReq->is_gid = DrvReq->is_name = DrvReq->no_uid = DrvReq->no_gid = DrvReq->no_name = DrvReq->no_host = DrvReq->no_vid = DrvReq->no_mode = DrvReq->no_date = DrvReq->no_time = DrvReq->no_age = 0;
	log(LOG_DEBUG,"vdqm_SetDedicate() : %s : regexp optim skipped\n", DrvReq->dedicate);
    return(0);
}

int vdqm_ResetDedicate(vdqm_drvrec_t *drv) {
    vdqmDrvReq_t *DrvReq;

    if ( drv == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    DrvReq = &drv->drv;

    if ( *DrvReq->dedicate != '\0' ) {
#if DEBUG
		log(LOG_DEBUG,"vdqm_ResetDedicate() : %s@%s : Reset dedication \"%s\"\n", drv->drv.drive, drv->drv.server, DrvReq->dedicate);
#endif
        if (drv->expbuf != NULL) free(drv->expbuf);
		drv->expbuf = NULL;
        *DrvReq->dedicate = '\0';
    }
    if ( *DrvReq->newdedicate != '\0' ) {
#if DEBUG
		log(LOG_DEBUG,"vdqm_ResetDedicate() : %s@%s : Reset reduced dedication \"%s\"\n", drv->drv.drive, drv->drv.server, DrvReq->newdedicate);
#endif
        if (drv->newexpbuf != NULL) free(drv->newexpbuf);
		drv->newexpbuf = NULL;
        *DrvReq->newdedicate = '\0';
    }
    return(0);
}

int vdqm_DrvMatch(vdqm_volrec_t *vol, vdqm_drvrec_t *drv) {
    vdqmVolReq_t *VolReq;
    char match_item[256], dummy[256];
    struct tm *current_tm, tmstruc;
    time_t clock;
    struct passwd *pw;
    int uid,gid,mode,age;
    char datestr[11], timestr[9], *name, *vid, *host; 
    char empty[1] = "";

    if ( vol == NULL || drv == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

	VolReq = &vol->vol;

	/*
     * If not dedicated, we match all
     */
    if ( *drv->drv.dedicate == '\0' ) {
#if DEBUG
		/* Call of log, even if logging nothing, takes some cycles */
		log(LOG_DEBUG,"vdqm_SetDedicate() : %s@%s : No dedication\n", drv->drv.drive, drv->drv.server);
#endif
		return(1);
	}

    if ( drv->drv.is_uid) {
#if DEBUG
		/* Call of log, even if logging nothing, takes some cycles */
		log(LOG_DEBUG,"vdqm_SetDedicate() : %s@%s : is_uid=%d : Binary match %d v.s. %d ?\n", drv->drv.drive, drv->drv.server, drv->drv.is_uid, VolReq->clientUID, drv->drv.uid);
#endif
		if (VolReq->clientUID !=  drv->drv.uid) return(0);
	}
    if ( drv->drv.is_gid) {
#if DEBUG
		log(LOG_DEBUG,"vdqm_SetDedicate() : %s@%s : Binary match %d v.s. %d ?\n", drv->drv.drive, drv->drv.server, VolReq->clientGID, drv->drv.gid);
#endif
		if (VolReq->clientGID !=  drv->drv.gid) return(0);
	}
	name = NULL;
    if ( drv->drv.is_name) {
		pw = Cgetpwuid(VolReq->clientUID);
		if ( pw == NULL ) {
			name = empty;
		} else {
			name = pw->pw_name;
		}
#if DEBUG
		log(LOG_DEBUG,"vdqm_SetDedicate() : %s@%s : String match \"%s\" v.s. \"%s\" ?\n", drv->drv.drive, drv->drv.server, name, drv->drv.name);
#endif
		if (strcmp(name,drv->drv.name) != 0) return(0);
	}


	/* No optimization was able to do the work. We do the full regexp */
	/* But taking ONLY relevant fields, e.g. those that are not equal */
	/* to ".*" */

    if ( ! (drv->drv.no_date && drv->drv.no_time && drv->drv.no_age)) {
		(void)time(&clock);
		if ( ! (drv->drv.no_date && drv->drv.no_time)) {
#if (defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32)
			(void)localtime_r(&clock,&tmstruc);
			current_tm = &tmstruc;
#else /* (_REENTRANT || _THREAD_SAFE) && !_WIN32 */
			current_tm = localtime(&clock);
#endif /* (_REENTRANT || _THREAD_SAFE) && !_WIN32 */
			if (! drv->drv.no_date) {
				(void) strftime(datestr,sizeof(datestr),VDQM_DEDICATE_DATEFORM,current_tm);
			}
			if (! drv->drv.no_time) {
				(void) strftime(timestr,sizeof(timestr),VDQM_DEDICATE_TIMEFORM,current_tm);
			}
		}
		if (! drv->drv.no_age) {
			age = (int)(clock - (time_t)VolReq->recvtime);
		}
	}
	if (! drv->drv.no_uid) {
		uid = VolReq->clientUID;
	}
	if (! drv->drv.no_gid) {
		gid = VolReq->clientGID;
	}
	if (! drv->drv.no_name) {
		if (name == NULL) {
			pw = Cgetpwuid(VolReq->clientUID);
			if ( pw == NULL ) {
				name = empty;
			} else {
				name = pw->pw_name;
			}
		}
	}
	if (! drv->drv.no_host) {
		host = VolReq->client_host;
	}
	if (! drv->drv.no_vid) {
		vid = VolReq->volid;
	}
	if (! drv->drv.no_mode) {
		mode = VolReq->mode;
	}

	match_item[0] = '\0';
	if (! drv->drv.no_uid) {
		sprintf(dummy,"uid=%d",uid);
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_gid) {
		sprintf(dummy,"gid=%d",gid);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_name) {
		sprintf(dummy,"name=%s",name);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_host) {
		sprintf(dummy,"host=%s",host);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_vid) {
		sprintf(dummy,"vid=%s",vid);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_mode) {
		sprintf(dummy,"mode=%d",mode);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_date) {
		sprintf(dummy,"datestr=%s",datestr);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_time) {
		sprintf(dummy,"timestr=%s",timestr);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}
	if (! drv->drv.no_age) {
		sprintf(dummy,"age=%d",age);
		if (match_item[0] != '\0') strcat(match_item,",");
		strcat(match_item,dummy);
	}

	if (drv->newexpbuf != NULL) {
#if DEBUG
		log(LOG_DEBUG,"vdqm_DrvMatch(): %s@%s : Reduced Regexp match \"%s\" v.s. \"%s\" ?\n",drv->drv.drive, drv->drv.server, match_item,drv->drv.newdedicate);
#endif
		if (Cregexp_exec(drv->newexpbuf, match_item) == 0) {
#if DEBUG
			log(LOG_DEBUG,"vdqm_DrvMatch() %s@%s : matched!\n", drv->drv.drive, drv->drv.server);
#endif
			return(1);
		}
	} else {
#if DEBUG
		log(LOG_DEBUG,"vdqm_DrvMatch(): %s@%s : Regexp match \"%s\" v.s. \"%s\" ?\n",drv->drv.drive, drv->drv.server, match_item,drv->drv.dedicate);
#endif
		if (Cregexp_exec(drv->expbuf, match_item) == 0) {
#if DEBUG
			log(LOG_DEBUG,"vdqm_DrvMatch() %s@%s : matched!\n", drv->drv.drive, drv->drv.server);
#endif
			return(1);
		}
	}
	return(0);
}
