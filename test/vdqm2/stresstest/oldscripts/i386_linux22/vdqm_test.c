#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <net.h>
#include <osdep.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>
#include <Ctape_constants.h>

#define CHECK_STATUS(X) {int _rc = X; if ( _rc < 0 ) fprintf(stderr,"(%s:%d) %s status %d: %s\n",__FILE__,__LINE__,#X,_rc,sstrerror(serrno));}
#define SYNTAX_ERROR(X) {fprintf(stderr,"(%s:%d) syntax error %s\n",__FILE__,__LINE__,X); sleep(1); goto newcmd;}
int main() {
    vdqmVolReq_t volreq;
    vdqmDrvReq_t drvreq;
    vdqmnw_t *nw;
	char volid[CA_MAXVIDLEN+1];
	char server[CA_MAXHOSTNAMELEN+1];
	char drive[CA_MAXUNMLEN+1];
	char dgn[CA_MAXDGNLEN+1];

	char cmds[][20] = VDQM_REQ_STRINGS;
/*
       {"VOLREQ","DRVREQ","DELVOL","DELDRV",
        "HOLD","RELEASE","GETVOL","GETDRV","SHUTDOWN","DEDICATE",""};
*/
	int icmds[] = VDQM_REQ_VALUES;
/*
                      {VDQM_VOL_REQ,VDQM_DRV_REQ,VDQM_DEL_VOLREQ,
                       VDQM_DEL_DRVREQ,VDQM_HOLD,VDQM_RELEASE,VDQM_GET_VOLQUEUE,
                       VDQM_GET_DRVQUEUE,VDQM_SHUTDOWN,VDQM_DEDICATE_DRV,-1};
*/
	char drvcmds[][20] = VDQM_STATUS_STRINGS;
	int idrvcmds[] = VDQM_STATUS_VALUES;
	char keywords[][20] = {"VOLID","DGN","SERVER","DRIVE",""};
	char *volkeyp[] = {NULL,NULL,NULL,NULL};
	char *drvkeyp[] = {NULL,NULL,NULL,NULL};

	char *p, *q;
	int i,j,status,value,mode,client_port,reqID,jobID,rc;

        char cmd[80];
	char cmdline[80];
        char dedicate[80];

    volkeyp[0] = drvkeyp[0] = volid;
    volkeyp[1] = drvkeyp[1] = dgn;
    volkeyp[2] = drvkeyp[2] = server;
    volkeyp[3] = drvkeyp[3] = drive;
	reqID = 0;
	client_port = 8889;
	for (;;) {
newcmd:
		fprintf(stdout,"VDQM request->");
		scanf("%s %[^\n]",cmd,cmdline);
		fflush(stdin);
		printf("command=%s %s\n",cmd,cmdline);
		p = strtok(cmd," ");
		if ( !strcmp(p,"EXIT") || !strcmp(p,"QUIT") ) exit(0);
                if ( !strcmp(p,"HELP") ) {
                    fprintf(stdout,"Possible commands are:\n");
                    for (i=0; icmds[i] != -1; i++ ) fprintf(stdout,"%s, ",cmds[i]);
                    fprintf(stdout,"EXIT, QUIT\n");
                    continue;
                }
		for (i=0; strlen(cmds[i]) && strcmp(cmds[i],p); i++);

		if ( !strlen(cmds[i]) ){
			fprintf(stdout,"No such command, %s\n",p);
			continue;
		} else {
			fprintf(stdout,"A %s command\n",cmds[i]);
		}
		p = strtok(cmdline," ");

        switch (icmds[i]) {
        case VDQM_SHUTDOWN:
        case VDQM_HOLD:
        case VDQM_RELEASE:
            CHECK_STATUS(vdqm_admin(NULL,icmds[i]));
            break;
        case VDQM_DEDICATE_DRV:
            memset(dgn,'\0',sizeof(dgn));
            memset(drive,'\0',sizeof(drive));
            memset(server,'\0',sizeof(server));
            memset(dedicate,'\0',sizeof(dedicate));
            if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
            printf("VDQM_DEDICATE_DRV(%s)\n",p);
            while ( p != NULL ) {
                if ( !strcmp("DEDICATE",p) ) {
                    p = strtok(NULL," ");
                    strcpy(dedicate,p);
                } else {
                    for (j=0; strlen(keywords[j]) && strcmp(keywords[j],p); j++);
                    if ( !strlen(keywords[j]) ) SYNTAX_ERROR(cmds[i]);
                    p=strtok(NULL," ");
                    if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
                    strcpy(volkeyp[j],p);
                }
                p=strtok(NULL," ");
            }
            fprintf(stdout,"call vdqm_DedicateDrive(NULL,%s,%s,%s,%s)\n",
                    dgn,server,drive,dedicate);    
            CHECK_STATUS(vdqm_DedicateDrive(NULL,dgn,server,drive,dedicate));
            fprintf(stdout,"return vdqm_DedicateDrive(NULL,%s,%s,%s,%s)\n",
                    dgn,server,drive,dedicate);
            break; 
        case VDQM_VOL_REQ:
            memset(volid,'\0',sizeof(volid));
			memset(dgn,'\0',sizeof(dgn));
			memset(drive,'\0',sizeof(drive));
			memset(server,'\0',sizeof(server));
			if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
			printf("VDQM_VOL_REQ (%s)\n",p);
			while ( p != NULL ) {
                                if ( !strcmp("MODE",p) ) {
                                        p = strtok(NULL," ");
                                        if ( *p == 'W' ) mode = WRITE_ENABLE;
                                        else mode = WRITE_DISABLE;
                                } else {
                                   for (j=0; strlen(keywords[j]) && strcmp(keywords[j],p); j++);
                                   if ( !strlen(keywords[j]) ) SYNTAX_ERROR(cmds[i]);
                                   p=strtok(NULL," ");
                                   if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
                                   strcpy(volkeyp[j],p);
                               }
                               p=strtok(NULL," ");
			}
			if ( *dgn == '\0' ) {
				fprintf(stderr,"DGN is required\n");
				SYNTAX_ERROR(cmds[i]);
			}
			reqID = 0;
			fprintf(stdout,"call vdqm_SendVolReq(NULL,0x%x,%s,%s,%s,%s,%d,%d)\n",
				reqID,volid,dgn,server,drive,mode,client_port);
			CHECK_STATUS(vdqm_SendVolReq(NULL,&reqID,volid,
				dgn,server,drive,mode,client_port));
			fprintf(stdout,"return vdqm_SendVolReq(NULL,0x%x,%s,%s,%s,%s,%d)\n",
				reqID,volid,dgn,server,drive,mode,client_port);
			break;
		case VDQM_DRV_REQ:
			memset(volid,'\0',sizeof(volid));
			memset(dgn,'\0',sizeof(dgn));
			memset(drive,'\0',sizeof(drive));
			memset(server,'\0',sizeof(server));
			status = value = 0;
			if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
			printf("VDQM_DRV_REQ (%s)\n",p);
			while ( p != NULL ) {
				if ( !strcmp("STATUS",p) ) {
					p=strtok(NULL," ");
                                        status = 0;
                                        while ( p != NULL ) {
                                                q = strtok(p,"|");
                                                while ( q != NULL ) {
                                                    fprintf(stderr,"Status string %s\n",q);
						    for (j=0; strlen(drvcmds[j]) && strcmp(drvcmds[j],q); j++);
                                                    fprintf(stderr,"j=%d\n",j);
						    if ( !strlen(drvcmds[j]) ) SYNTAX_ERROR(cmds[i]);
						    status |= idrvcmds[j];
                                                    q = strtok(NULL,"|");
                                                }
                                                p = q;
					}
				} else if ( !strcmp("REQID",p) ) {
					p = strtok(NULL," ");
					value = atoi(p);
                                } else if ( !strcmp("JOBID",p) ) {
					p = strtok(NULL," ");
					jobID = atoi(p);
                                } else if ( !strcmp("MBYTES",p) ) {
                                        p = strtok(NULL," ");
                                        value = atoi(p);
				} else {
					for (j=0; strlen(keywords[j]) && strcmp(keywords[j],p); j++);
					if ( !strlen(keywords[j]) ) SYNTAX_ERROR(cmds[i]);
					p=strtok(NULL," ");
					if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
					strcpy(drvkeyp[j],p);
				}
				p=strtok(NULL," ");
			}
			if ( *dgn == '\0' && status != VDQM_TPD_STARTED ) {
				fprintf(stderr,"DGN is required\n");
				SYNTAX_ERROR(cmds[i]);
			}

			fprintf(stdout,"call vdqm_UnitStatus(NULL,%s,%s,%s,%s,0x%x,0x%x,%d)\n",
				volid,dgn,server,drive,status,value,jobID);
			CHECK_STATUS(vdqm_UnitStatus(NULL,volid,dgn,server,drive,
				&status,&value,jobID));
			fprintf(stdout,"return vdqm_UnitStatus(NULL,%s,%s,%s,%s,0x%x,0x%x,%d)\n",
				volid,dgn,server,drive,status,value,jobID);
			if ( status & VDQM_VOL_UNMOUNT ) fprintf(stdout,
				"\t->>>>> UNMOUNT VOLUME %s <<<<<-\n",volid);
			break;
        case VDQM_DEL_VOLREQ:
			memset(volid,'\0',sizeof(volid));
			memset(dgn,'\0',sizeof(dgn));
			memset(drive,'\0',sizeof(drive));
			memset(server,'\0',sizeof(server));
            		reqID = 0;
			if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
			printf("VDQM_DEL_VOLREQ (%s)\n",p);
			while ( p != NULL ) {
       	  	 		if ( !strcmp("PORT",p) ) {
					p = strtok(NULL," ");
					value = atoi(p);
				} else if ( !strcmp("REQID",p) ) {
					p = strtok(NULL," ");
					reqID = atoi(p);
				} else {
                        	        for (j=0; strlen(keywords[j]) && strcmp(keywords[j],p); j++);
                                	if ( !strlen(keywords[j]) ) SYNTAX_ERROR(cmds[i]);

 	   				p=strtok(NULL," ");
    					if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
    					strcpy(volkeyp[j],p);
	                	}
				p=strtok(NULL," ");
			}
		if ( reqID == 0 ) {
			fprintf(stderr,"REQID is required\n");
			SYNTAX_ERROR(cmds[i]);
		}
            fprintf(stdout,"call vdqm_DelVolumeReq(NULL,%d,%s,%s,%s,%s,%d)\n",
                reqID,volid,dgn,server,drive,value);
            CHECK_STATUS(vdqm_DelVolumeReq(NULL,reqID,volid,dgn,server,drive,
                value));
            fprintf(stdout,"return vdqm_DelVolumeReq(NULL,%d,%s,%s,%s,%s,%d)\n",
                reqID,volid,dgn,server,drive,value);
            break;
        case VDQM_DEL_DRVREQ:
			memset(volid,'\0',sizeof(volid));
			memset(dgn,'\0',sizeof(dgn));
			memset(drive,'\0',sizeof(drive));
			memset(server,'\0',sizeof(server));
			status = value = 0;
			if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
			printf("VDQM_DEL_DRVREQ (%s)\n",p);
			while ( p != NULL ) {
				for (j=0; strlen(keywords[j]) && strcmp(keywords[j],p); j++);
				if ( !strlen(keywords[j]) ) SYNTAX_ERROR(cmds[i]);
				p=strtok(NULL," ");
				if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
				strcpy(drvkeyp[j],p);
				p=strtok(NULL," ");
			}
			if ( *dgn == '\0' ) {
				fprintf(stderr,"DGN is required\n");
				SYNTAX_ERROR(cmds[i]);
			}
            fprintf(stdout,"call vdqm_DelDrive(NULL,%s,%s,%s)\n",
                dgn,server,drive);
            CHECK_STATUS(vdqm_DelDrive(NULL,dgn,server,drive));
            fprintf(stdout,"return vdqm_DelDrive(NULL,%s,%s,%s)\n",
                dgn,server,drive);
            break;
        case VDQM_GET_VOLQUEUE:
            memset(&volreq,'\0',sizeof(volreq));
			memset(volid,'\0',sizeof(volid));
			memset(dgn,'\0',sizeof(dgn));
			memset(drive,'\0',sizeof(drive));
			memset(server,'\0',sizeof(server));
            nw = NULL;
			status = value = 0;
			while ( p != NULL ) {
				for (j=0; strlen(keywords[j]) && strcmp(keywords[j],p); j++);
				if ( !strlen(keywords[j]) ) SYNTAX_ERROR(cmds[i]);
				p=strtok(NULL," ");
				if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
				strcpy(volkeyp[j],p);
				p=strtok(NULL," ");
			}
            if ( *dgn != '\0' && strcmp(dgn,"ALL") ) strcpy(volreq.dgn,dgn);
            if ( *server != '\0' ) strcpy(volreq.server,server);
            fprintf(stdout,"call vdqm_NextVol(0x%x,0x%x) (dgn=%s)\n",
                &nw,&volreq,volreq.dgn);
            while ( (rc = vdqm_NextVol(&nw,&volreq)) != -1 ) {
                if ( volreq.VolReqID <= 0 ) continue;
                fprintf(stdout,"\tID=%d, time=%d, VID=%s, uid=%d, gid=%d, host=%s\n",
                    volreq.VolReqID,volreq.recvtime,volreq.volid,volreq.clientUID,
                    volreq.clientGID,volreq.client_host);
            }
            fprintf(stdout,"return vdqm_NextVol(0x%x,0x%x)\n",
                &nw,&volreq);
            break;
        case VDQM_GET_DRVQUEUE:
            memset(&drvreq,'\0',sizeof(drvreq));
			memset(volid,'\0',sizeof(volid));
			memset(dgn,'\0',sizeof(dgn));
			memset(drive,'\0',sizeof(drive));
			memset(server,'\0',sizeof(server));
            nw = NULL;
			status = value = 0;
			while ( p != NULL ) {
				for (j=0; strlen(keywords[j]) && strcmp(keywords[j],p); j++);
				if ( !strlen(keywords[j]) ) SYNTAX_ERROR(cmds[i]);
				p=strtok(NULL," ");
				if ( p == NULL ) SYNTAX_ERROR(cmds[i]);
				strcpy(drvkeyp[j],p);
				p=strtok(NULL," ");
			}
            if ( *dgn != '\0' && strcmp(dgn,"ALL") ) strcpy(drvreq.dgn,dgn);
            if ( *server != '\0' ) strcpy(drvreq.server,server);
            fprintf(stdout,"call vdqm_NextDrive(0x%x,0x%x)\n",
                &nw,&drvreq);
            while ( (rc = vdqm_NextDrive(&nw,&drvreq)) != -1 ) {
                if ( *drvreq.server != '\0' && *drvreq.drive != '\0' ) {
                  fprintf(stdout,"\nDGN:%s ID=%d, time=%d, VID=%s(%d), server=%s, drive=%s used: %d, Last MB: %d, Tot. MB: %d, #Errors: %d\n\t\tstatus: ",
                    drvreq.dgn,
                    drvreq.VolReqID,drvreq.recvtime,drvreq.volid,drvreq.mode,
                    drvreq.server,drvreq.drive,drvreq.usecount,drvreq.MBtransf,
                    (int)drvreq.TotalMB,drvreq.errcount);
                  if ( *drvreq.dedicate != '\0' )
                      fprintf(stdout,"\tDrive is dedicated to %s\n",
                              drvreq.dedicate);
                  j=0;
                  while ( *drvcmds[j] != '\0' ) {
                     if ( drvreq.status & idrvcmds[j] ) fprintf(stdout,"%s |",
                       drvcmds[j]);
                     j++;
                  }
                  fprintf(stdout,"\n");
               }
            }
            fprintf(stdout,"return vdqm_NextDrive(0x%x,0x%x) (dgn=%s)\n",
                &nw,&drvreq,drvreq.dgn);
            break;
       }
       }
	return(0);
}
