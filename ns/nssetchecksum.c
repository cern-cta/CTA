/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nssetchecksum.c,v $ $Revision: 1.2 $ CERN IT-ADC-CA Ben Couturier";
#endif /* not lint */

/*
 * Utility to set/reset the tape segment checksum in the name server
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <Cns_api.h>
#include <Cgetopt.h>
#include <serrno.h>

#define OPT_COPYNO 10
#define OPT_SEGMENTNO 11
#define OPT_CHECKSUM_NAME 12
#define OPT_CHECKSUM 13
#define OPT_UPDATE 14
#define OPT_CLR 15


void usage(char *name) {
    printf("Usage: %s --copyno copy_nb --segmentno segment_nb\n", name);
    printf("          [--update] --checksum_name name --checksum value file_name\n");
    printf("Or:    %s --copyno copy_nb --segmentno segment_nb --clr\n", name);
}


int main(int argc, char *argv[]) {
    struct Cns_filestat stat;
    struct Cns_segattrs rsat, *outsat;
    int outnb, i, rc;
    unsigned int targetcopyno, targetfsec, targetchecksum, targetfound=0;
    int hastgcopyno = 0, hastgfsec=0, hastgchecksum=0, hastgchecksum_name =0;
    int clrflag = 0, updateflag = 0;
    char targetchecksum_name[CA_MAXCKSUMNAMELEN+1];
    int errflg = 0;
    char *dp, *filename, c;
  

    static struct Coptions longopts[] = {
        {"copyno", REQUIRED_ARGUMENT, 0, OPT_COPYNO},
        {"segmentno", REQUIRED_ARGUMENT, 0, OPT_SEGMENTNO},
        {"checksum_name", REQUIRED_ARGUMENT, 0, OPT_CHECKSUM_NAME},
        {"checksum", REQUIRED_ARGUMENT, 0, OPT_CHECKSUM},
        {"update", NO_ARGUMENT, 0, OPT_UPDATE},
        {"clr", NO_ARGUMENT, 0, OPT_CLR},
        {0, 0, 0, 0}
    };

    Copterr = 1;
    Coptind = 1;
    while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
        switch (c) {
        case OPT_COPYNO:
            if ((targetcopyno = strtol (Coptarg, &dp, 10)) <= 0 ||
                *dp != '\0') {
                fprintf (stderr, "invalid copy number %s\n", Coptarg);
                errflg++;
            }
            hastgcopyno=1;
            break;
        case OPT_SEGMENTNO:
            if ((targetfsec = strtol (Coptarg, &dp, 10)) <= 0 ||
                *dp != '\0') {
                fprintf (stderr, "invalid segment number %s\n", Coptarg);
                errflg++;
            }
            hastgfsec=1;
            break;
        case OPT_CHECKSUM_NAME:
            strncpy(targetchecksum_name, Coptarg, CA_MAXCKSUMNAMELEN);
            targetchecksum_name[CA_MAXCKSUMNAMELEN]='\0';
            hastgchecksum_name=1;
            break;
        case OPT_CHECKSUM:
            if ((targetchecksum = strtoul (Coptarg, &dp, 16)) <= 0 ||
                *dp != '\0') {
                fprintf (stderr, "invalid checksum %s\n", Coptarg);
                errflg++;
            }
            hastgchecksum=1;
            break;
        case OPT_UPDATE:
            updateflag = 1;
            break;
        case OPT_CLR:
            clrflag = 1;
            break;
        default:
            break;
        }
    }

    if (Coptind >= argc
        || !(hastgcopyno && hastgfsec)
        || (!clrflag && !(hastgchecksum && hastgchecksum_name))) {
        errflg++;
    }

    if ((clrflag && updateflag)
        || (clrflag && hastgchecksum)
        || (clrflag && hastgchecksum_name)) {
        fprintf(stderr, "Cannot specify --clr and value for checksum!\n");
        usage(argv[0]);
        exit(USERR);
    }
  
    if (errflg) {
        usage(argv[0]);
        exit(USERR);
    }

    filename = argv[Coptind];
      
    rc = Cns_stat(filename, &stat);
    if (rc != 0) {
        fprintf(stderr, "Cns_stat error %d : %s\n", 
                serrno, 
                sstrerror(serrno));
        return EXIT_FAILURE;
    }
  
  
    rc = Cns_getsegattrs(filename,
                         NULL,
                         &outnb,
                         &outsat);
  
    if (rc != 0) {
        fprintf(stderr, "Cns_getsegattrs error %d : %s\n", 
                serrno, 
                sstrerror(serrno));
        return EXIT_FAILURE;
    }

    for (i=0; i<outnb; i++) {
        if (outsat[i].copyno == targetcopyno
            && outsat[i].fsec == targetfsec) {
            targetfound = 1;

            rsat = outsat[i];
            if (clrflag) {
                rsat.checksum_name[0] = '\0';
                rsat.checksum = 0;
            } else {
                strcpy(rsat.checksum_name, targetchecksum_name);
                rsat.checksum = targetchecksum;
            }
          
            if (updateflag) {
                rc = Cns_updateseg_checksum(NULL, stat.fileid,
                                            &(outsat[i]), &rsat);
                if (rc != 0) {
                    fprintf(stderr, "Cns_updateseg_checksum error %d : %s\n", 
                            serrno, 
                            sstrerror(serrno));
                }
            } else {
                rc = Cns_replaceseg(NULL, stat.fileid,
                                    &(outsat[i]), &rsat);
                if (rc != 0) {
                    fprintf(stderr, "Cns_replaceseg_checksum error %d : %s\n", 
                            serrno, 
                            sstrerror(serrno));
                }
              
            }
                  
            return EXIT_FAILURE;
        }
          
    }

  
    if (outnb > 0) {
        free(outsat);
    }
  
    if (!targetfound) {
        fprintf(stderr, "Could not find segment !\n");
        return EXIT_FAILURE;
    }
    
    return EXIT_SUCCESS;
 
}
