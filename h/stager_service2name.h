/* $Id: stager_service2name.h,v 1.2 2005/03/17 15:01:19 jdurand Exp $ */

#ifndef __stager_service2name_h
#define __stager_service2name_h

char *service2name[STAGER_SERVICE_MAX] = { "STAGER_SERVICE_DB",
					   "STAGER_SERVICE_QUERY",
					   "STAGER_SERVICE_UPDATE",
					   "STAGER_SERVICE_GETNEXT",
					   "STAGER_SERVICE_JOB",
					   "STAGER_SERVICE_GC"
};

#endif /* __stager_service2name_h */
