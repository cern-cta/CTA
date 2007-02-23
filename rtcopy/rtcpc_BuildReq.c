/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpc_BuildReq.c - build the tape and file request list from command
 */


#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Ctape_api.h>
#include <Ctape.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <serrno.h>

extern char *optarg;
extern int optind;

static int rtcpc_A_opt(int , const char *, tape_list_t **);
static int rtcpc_b_opt(int , const char *, tape_list_t **);
static int rtcpc_c_opt(int , const char *, tape_list_t **);
static int rtcpc_C_opt(int , const char *, tape_list_t **);
static int rtcpc_d_opt(int , const char *, tape_list_t **);
static int rtcpc_E_opt(int , const char *, tape_list_t **);
static int rtcpc_f_opt(int , const char *, tape_list_t **);
static int rtcpc_F_opt(int , const char *, tape_list_t **);
static int rtcpc_g_opt(int , const char *, tape_list_t **);
static int rtcpc_G_opt(int , const char *, tape_list_t **);
static int rtcpc_i_opt(int , const char *, tape_list_t **);
static int rtcpc_I_opt(int , const char *, tape_list_t **);
static int rtcpc_l_opt(int , const char *, tape_list_t **);
static int rtcpc_L_opt(int , const char *, tape_list_t **);
static int rtcpc_n_opt(int , const char *, tape_list_t **);
static int rtcpc_N_opt(int , const char *, tape_list_t **);
static int rtcpc_o_opt(int , const char *, tape_list_t **);
static int rtcpc_q_opt(int , const char *, tape_list_t **);
static int rtcpc_s_opt(int , const char *, tape_list_t **);
static int rtcpc_S_opt(int , const char *, tape_list_t **);
static int rtcpc_t_opt(int , const char *, tape_list_t **);
static int rtcpc_T_opt(int , const char *, tape_list_t **);
static int rtcpc_v_opt(int , const char *, tape_list_t **);
static int rtcpc_V_opt(int , const char *, tape_list_t **);
static int rtcpc_x_opt(int , const char *, tape_list_t **);
static int rtcpc_X_opt(int , const char *, tape_list_t **);
static int rtcpc_Z_opt(int , const char *, tape_list_t **);
static int rtcpc_diskfiles(int , const char *, tape_list_t **);

#define BIND_OPT(X,Y,Z) {strcat(opts,#Z); optlist[X-'A'] = (int (*)(int , const char *, tape_list_t **))rtcpc_##Y##_opt;};
#define OPT_MAX  'z'-'A'+1

int DLL_DECL rtcpc_BuildReq(tape_list_t **tape, int argc, char *argv[]) {
    static int (*optlist[OPT_MAX])(int, const char *, tape_list_t **);
    static int recursion_level = 0;
    char opts[OPT_MAX] = "";
    int i,j,rc, local_optind, local_repeated[OPT_MAX];
    int c;
    int mode;
    extern int getopt();
    
    recursion_level++;
    for (i=0; i<OPT_MAX; i++) optlist[i] = 0;
    
    BIND_OPT('A',A,A:);
    BIND_OPT('b',b,b:);
    BIND_OPT('c',c,c:);
    BIND_OPT('C',C,C:);
    BIND_OPT('d',d,d:);
    BIND_OPT('E',E,E:);
    BIND_OPT('f',f,f:);
    BIND_OPT('F',F,F:);
    BIND_OPT('g',g,g:);
    BIND_OPT('G',G,G);
    BIND_OPT('i',i,i:);
    BIND_OPT('I',I,I);
    BIND_OPT('l',l,l:);
    BIND_OPT('L',L,L:);
    BIND_OPT('n',n,n);
    BIND_OPT('N',N,N:);
    BIND_OPT('o',o,o);
    BIND_OPT('q',q,q:);
    BIND_OPT('s',s,s:);
    BIND_OPT('S',S,S:);
    BIND_OPT('t',t,t:);
    BIND_OPT('T',T,T);
    BIND_OPT('v',v,v:);
    BIND_OPT('V',V,V:);
    BIND_OPT('x',x,x);
    BIND_OPT('X',X,X:);
    BIND_OPT('Z',Z,Z:);

    if ( strstr(argv[0],"tpwrite") != NULL ) mode = WRITE_ENABLE;
    else mode = WRITE_DISABLE;

    /*
     * We need to do two passes over the arguments. In the first
     * pass the file list elements are created given the -q option
     * and the disk files specified. All recursive levels of
     * input option files are also checked.
     */
    optind = 1;
    rc = 0;
    for (j=0; j<OPT_MAX; j++) local_repeated[j] = 0;

    while ( (c = getopt(argc,argv,opts)) != EOF ) {
        if ( c != 'q' && c != 'i' && c != 'x' ) continue;
        if ( c == '?' ) return(-1);
        if ( local_repeated[c-'A'] != 0 ) {
            rtcp_log(LOG_ERR,"REPEATED OPTION -%c NOT ALLOWED\n",c);
            serrno = EINVAL;
            return(-1);
        }
        local_optind = optind;
        if ( (rc = (*optlist[c-'A'])(mode,optarg,tape)) == -1 ) return(-1);
        local_repeated[c-'A']++;
        optind = local_optind;
    }

    for (j=optind; j<argc; j++) {
        if ( (rc = rtcpc_diskfiles(mode,argv[j],tape)) == -1 ) return(-1);
    }

    /*
     * In the second pass all other options are checked and the
     * file list elements created in the first pass are filled.
     */
    optind = 1;
    rc = 0;
    for (j=0; j<OPT_MAX; j++) local_repeated[j] = 0;

    while ( (c = getopt(argc,argv,opts)) != EOF ) {
        if ( c == 'q' || c == 'i' || c == 'x' ) continue;
        if ( c == '?' ) return(-1);
        /*
         * -E option is allowed to be repeated....
         */
        if ( local_repeated[c-'A'] != 0 && c != 'E' ) {
            rtcp_log(LOG_ERR,"REPEATED OPTION -%c NOT ALLOWED\n",c);
            serrno = EINVAL;
            return(-1);
       }
        if ( (rc = (*optlist[c-'A'])(mode,optarg,tape)) == -1 ) return(-1);
        local_repeated[c-'A']++;
    }

    /*
     * Finally, if we are at top of the recursion (i.e. we were
     * the first to be called) set the disk file sequence numbers.
     */
    if ( recursion_level == 1 ) rc = rtcpc_diskfiles(mode,NULL,tape);
    recursion_level--;
    return(rc);
}

void rtcpc_InitReqStruct(rtcpTapeRequest_t *tapereq,
                         rtcpFileRequest_t *filereq) {
    if ( tapereq != NULL ) {
        memset(tapereq,'\0',sizeof(rtcpTapeRequest_t));
        tapereq->err.max_tpretry = -1;
        tapereq->err.max_cpretry = -1;
        tapereq->err.severity = RTCP_OK;
    }
    if ( filereq != NULL ) {
        memset(filereq,'\0',sizeof(rtcpFileRequest_t));
        filereq->VolReqID = -1;
        filereq->jobID = -1;
        filereq->stageSubreqID = -1;
        filereq->position_method = -1;
        filereq->tape_fseq = -1;
        filereq->disk_fseq = -1;
        filereq->blocksize = -1;
        filereq->recordlength = -1;
        filereq->retention = -1;
        filereq->def_alloc = -1;
        filereq->rtcp_err_action = -1;
        filereq->tp_err_action = -1;
        filereq->convert = -1;
        filereq->check_fid = -1;
        filereq->concat = -1;
        filereq->err.max_tpretry = -1;
        filereq->err.max_cpretry = -1;
        filereq->err.severity = RTCP_OK;
    }
    return;
}

static int newTapeList(tape_list_t **tape, tape_list_t **newtape,
                       int mode) {
    tape_list_t *tl;
    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tl = (tape_list_t *)calloc(1,sizeof(tape_list_t));
    if ( tl == NULL ) return(-1);
    rtcpc_InitReqStruct(&tl->tapereq,NULL);
    tl->tapereq.mode = mode;
    if ( *tape != NULL ) {
        CLIST_INSERT((*tape)->prev,tl);
    } else {
        CLIST_INSERT(*tape,tl);
    }
    if ( newtape != NULL ) *newtape = tl;
    return(0);
}

int DLL_DECL rtcp_NewTapeList(tape_list_t **tape, tape_list_t **newtape,
                              int mode) {
    return(newTapeList(tape,newtape,mode));
}

static int newFileList(tape_list_t **tape, file_list_t **newfile,
                       int mode) {
    file_list_t *fl;
    rtcpFileRequest_t *filereq;
    int rc;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    fl = (file_list_t *)calloc(1,sizeof(file_list_t));
    if ( fl == NULL ) return(-1);
    filereq = &fl->filereq;
    rtcpc_InitReqStruct(NULL,filereq);

    /*
     * Insert at end of file request list
     */
    CLIST_INSERT((*tape)->file,fl);

    fl->tape = *tape;
    if ( newfile != NULL ) *newfile = fl;
    return(0);
}

int DLL_DECL rtcp_NewFileList(tape_list_t **tape, file_list_t **newfile,
                              int mode) {
    return(newFileList(tape,newfile,mode));
}

static int rtcpc_A_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;


    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;

    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
         * Set option if not already set
         */
        if ( filereq->def_alloc == -1 ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            if ( strcmp(p,"deferred") == 0 ) 
                filereq->def_alloc = 1;
            else if ( strcmp(p,"immediate") == 0 ) 
                filereq->def_alloc = 0;
            else {
                rtcp_log(LOG_ERR,"INVALID VALUE %s FOR -A OPTION\n",p);
                rc = -1;
                serrno = EINVAL;
                break;
            }
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);

    return(rc);
}

static int rtcpc_b_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    int blocksize = -1;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
         * Set option
         */
        if ( filereq->blocksize == -1 ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            blocksize = atoi(p);
            if ( blocksize == 0 ) {
                rtcp_log(LOG_ERR,"Block size cannot be equal to zero.\n");
                serrno = EINVAL;
                rc = -1;
                break;
            }
            filereq->blocksize = blocksize;
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);

    return(rc);
}

/*
 * This is current a noop. The switch was forseen for a stager
 * development to enable "blind" copy of a tape, ie. when the number
 * of tape files is unknown on a tpread. The development was never
 * finalised but the option is kept reserved for future continuation
 * of this development.
 */
static int rtcpc_c_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc = 0;
    return(rc);
}

static int rtcpc_C_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc,i;
    int convert = -1;
    char *p, *q, *local_value;
    char valid_convs[][7] = {"block","ascii","ebcdic",""};
    char conv_values[] = {FIXVAR,ASCCONV,EBCCONV};
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        strcpy(local_value,value);
        p = local_value;
        filereq = &fl->filereq;
        /*
        * Set option
        */
        if ( filereq->convert == -1 ) {
            if ( p != NULL ) convert = 0;
            do {
                if ( (q = strstr(p,",")) != NULL ) *q = '\0';
                i=0;
                while ( *valid_convs[i] != '\0' && strcmp(p,valid_convs[i]) ) i++;
                if ( *valid_convs[i] == '\0' ) {
                    rtcp_log(LOG_ERR,"INVALID CONVERSION SPECIFIED\n");
                    serrno = EINVAL;
                    rc = -1;
                    break;
                }
                convert |= conv_values[i];
                if ( q != NULL ) p = q+1;
            } while ( q != NULL );
            if ( !(convert & (ASCCONV | EBCCONV)) ) convert |= ASCCONV;
            
            filereq->convert = convert;
            if ( !VALID_CONVERT(filereq) ) {
                rtcp_log(LOG_ERR,"INVALID CONVERSION SPECIFIED\n");
                serrno = EINVAL;
                rc = -1;
                break;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}
static int rtcpc_d_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    rtcpTapeRequest_t *tapereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;

    if ( rc != -1 ) {
        /*
         * Set option
         */
        if ( strlen(value) > CA_MAXDENLEN ) {
            rtcp_log(LOG_ERR,"INVALID DENSITY SPECIFIED\n");
            serrno = EINVAL;
            rc = -1;
        }

        if ( rc != -1 ) strcpy(tapereq->density,value);
    }

    return(rc);
}
static int rtcpc_E_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    int rtcp_err_action = 0;
    int tp_err_action = 0;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    int i;
    char erropts[][10] = { "skip", "keep", "ignoreeoi", "" };
    int erropt_values[] = {SKIPBAD,KEEPFILE,IGNOREEOI};

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        q = p = local_value;
        filereq = &fl->filereq;
        /*
        * Set option
        */
        while ( q != NULL ) {
            if ( (q = strstr(p,",")) != NULL ) *q = '\0';
            if ( p != NULL ) {
                i=0;
                while ( *erropts[i] != '\0' && strcmp(p,erropts[i])) i++;
                if ( *erropts[i] == '\0' ) {
                    rtcp_log(LOG_ERR,"%s IS NOT A VALID OPTIN FOR -E\n",p);
                    serrno = EINVAL;
                    rc = -1;
                    break;
                }
                if ( i<2 ) rtcp_err_action = erropt_values[i];
                else tp_err_action = erropt_values[i];
            } else if ( rtcp_err_action == 0 ) 
                rtcp_err_action = KEEPFILE;
            if ( filereq->rtcp_err_action == -1 &&
                 rtcp_err_action > 0 ) filereq->rtcp_err_action = 0;
            if ( filereq->tp_err_action == -1 && tp_err_action > 0 ) 
                filereq->tp_err_action = 0;
            filereq->rtcp_err_action |= rtcp_err_action;
            filereq->tp_err_action |= tp_err_action;
            
            if ( q != NULL ) {
                *q = ',';
                p = q;
                p++;
            }
        }
        if ( rc == -1 ) break;
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}
static int rtcpc_f_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    char *p, *q, *local_value,fid[CA_MAXFIDLEN+1];
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;


    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set option if not already set
        */
        if ( *filereq->fid == '\0' ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            if ( strlen(p) > CA_MAXFIDLEN ) p += strlen(p) - CA_MAXFIDLEN;
            if ( p != NULL ) strcpy(fid,p);
            strcpy(filereq->fid,fid);
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}

static int rtcpc_F_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc,i;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char valid_recfm[][10] = {"U,f77","U","U,bin","F","FB","FBS",
        "FS","F,-f77",""};
    char set_recfm[][2] =    {"U",    "U","U",    "F","F", "F",
        "F", "F"};

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set option if not already set
        */
        if ( *filereq->recfm == '\0' ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            if ( p != NULL ) {
                i=0;
                while (*valid_recfm[i] != '\0' &&
                    strcmp(p,valid_recfm[i]) ) i++;
                if ( *valid_recfm[i] == '\0' ) {
                    rtcp_log(LOG_ERR,
                        "INVALID FORMAT SPECIFIED\n");
                    serrno = EINVAL;
                    rc = -1;
                    break;
                }
            }
            
            if ( (strcmp(valid_recfm[i],"F,-f77") == 0) ||
                 (strcmp(valid_recfm[i],"U,bin")  == 0) ) {
                if ( filereq->convert == -1 ) filereq->convert = NOF77CW; 
                else filereq->convert |= NOF77CW;
            }
            
            if ( (strcmp(set_recfm[i],"U") == 0) &&
                 (strcmp(valid_recfm[i],"U,bin")  != 0) ) 
                filereq->recordlength = 0;
            
            strcpy(filereq->recfm,set_recfm[i]);
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}

static int rtcpc_g_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    rtcpTapeRequest_t *tapereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;

    if ( rc != -1 ) {
        /*
         * Set option
         */
        if ( strlen(value) > CA_MAXDGNLEN ) {
            rtcp_log(LOG_ERR,"INVALID DEVICE GROUP SPECIFIED\n");
            serrno = EINVAL;
            rc = -1;
        }
        if ( rc != -1 ) strcpy(tapereq->dgn,value);
    }
    return(rc);
}

/*
 * Group user concept for foreign systems not supported
 * any longer (was mainly intended for CRAY tape servers and/or
 * VMS client in the old SHIFT era). The switch is still accepted
 * but there is no action
 */
static int rtcpc_G_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc = 0;
    return(rc);
}
/*
 * New -i input.file option to support an input request file for
 * very large requests. Note: this routine makes reentrant calls
 * to rtcpc_BuildReq(), one for each record in the file.
 */
static int rtcpc_i_opt(int mode,
                       const char *value,
                       tape_list_t **tape) {
    FILE *fd;
    tape_list_t *tmp_tape;
    rtcpTapeRequest_t *tmp_tapereq, *tapereq;
    int rc, i, status;
    char input_file[CA_MAXPATHLEN+1];
    static char last_input_file[CA_MAXPATHLEN+1];
    char input_line[CA_MAXLINELEN+1];
    static char cmds[][10] = {"tpread","tpwrite",""};
    char *p, *q;
    char **argv = NULL;
    int argc = 0;

    if ( tape == NULL || value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    rc = 0;
    if ( strlen(value) > CA_MAXPATHLEN ) {
        rtcp_log(LOG_ERR,"TOO LONG PATH NAME GIVEN WITH -i OPTION\n");
        serrno = EINVAL;
        return(-1);
    }
    strcpy(input_file,value);
    if ( strcmp(input_file,last_input_file) == 0 ) {
        rtcp_log(LOG_ERR,"RECURSIVE SPECIFICATION OF INPUT FILE %s\n",
            input_file);
        serrno = EINVAL;
        return(-1);
    }
    fd = fopen(input_file,"r");
    if ( fd == NULL ) {
        rtcp_log(LOG_ERR,"CANNOT OPEN INPUT OPTION FILE %s: %s\n",
            input_file,sstrerror(errno));
        serrno = ENOENT;
        return(-1);
    }
    while ( (status = fscanf(fd,"%[^\n]\n",input_line)) > 0 ) {
        p = input_line;
        argc = 1; /* one for the command */
        if ( *p != '#' ) argc++;
        while ( (q = strpbrk(p," \t")) != NULL ) {
            if ( *p == '#' ) break;
            argc++;
            p = q+1;
        }
        if ( argc > 1 ) {
            argv = (char **)malloc((argc+1) * sizeof(char *));
            if ( argv == NULL ) {
                rc = -1;
                serrno = errno;
                break;
            }
            if ( mode == WRITE_ENABLE ) argv[0] = cmds[1];
            else argv[0] = cmds[0];
            p = input_line;
            for (i=1; i<argc; i++) {
                q = strpbrk(p," \t");
                if ( q != NULL ) *q = '\0';
                argv[i] = (char *)calloc(1,strlen(p)+1);
                if ( argv[i] == NULL ) {
                    rc = -1;
                    serrno = errno;
                    break;
                }
                strcpy(argv[i],p);
                p = q+1;
            }
            argv[argc] = NULL;
            strcpy(last_input_file,input_file);
            tmp_tape = NULL;
            if ( rc != -1 ) {
                rtcp_log(LOG_DEBUG,"recursive call rtcpc_BuildReq() with argc=%d, argv=",
                    argc);
                for (i=1; i<argc; i++) rtcp_log(LOG_DEBUG,"%s ",argv[i]);
                rtcp_log(LOG_DEBUG,"%s","\n");
                rc = rtcpc_BuildReq(&tmp_tape,argc,argv);
                if ( rc != -1 ) {
                    /*
                     * Merge tape lists
                     */
                    tmp_tapereq = &tmp_tape->tapereq;
                    if ( *tape != NULL ) tapereq = &(*tape)->tapereq;
                    else tapereq = tmp_tapereq;
                    if (strcmp(tmp_tapereq->vid,tapereq->vid) != 0 &&
                        *tmp_tapereq->vid != '\0' && *tapereq->vid != '\0') {
                        rtcp_log(LOG_ERR,"Specify new VID (%s) in input option file is not allowed\n",
                            tmp_tapereq->vid);
                        serrno = EINVAL;
                        rc = -1;
                    } else {
                        if ( *tape == NULL ) *tape = tmp_tape;
                        else CLIST_INSERT((*tape)->file,tmp_tape->file);
                    }
                }
            }
            for (i=1; i<argc; i++) free(argv[i]);
            free(argv);
            if ( rc == -1 ) break;
        }
    }
    if ( rc == 0 && status != EOF ) {
        rtcp_log(LOG_ERR,"ERROR READING INPUT OPTION FILE %s: %s\n",
            input_file,sstrerror(errno));
        rc = -1;
    }
    fclose(fd);

    *last_input_file = '\0';
    return(rc);
}

/*
 * Support for tpread -I ... to provide device queue information only.
 */
static int rtcpc_I_opt(int mode, 
                       const char *value,
                       tape_list_t **tape) {
    int rc;

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    /*
     * We distinguish info. requests from normal read/write requests
     * by setting the tape mode to RTCP_INFO_REQ;
     */
    (*tape)->tapereq.mode = RTCP_INFO_REQ;

    return(0);
}

static int rtcpc_l_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    rtcpTapeRequest_t *tapereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;

    if ( rc != -1 ) {
        /*
         * Set option
         */
        if ( strlen(value) > CA_MAXLBLTYPLEN ) {
            rtcp_log(LOG_ERR,"INVALID LABEL TYPE\n");
            serrno = EINVAL;
            rc = -1;
        }
        if ( rc != -1 ) strcpy(tapereq->label,value);
    }
    return(rc);
}
static int rtcpc_L_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    int recordlength = -1;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set option
        */
        if ( filereq->recordlength == -1 ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            recordlength = atoi(p);
            if ( recordlength <= 0 ) {
                rtcp_log(LOG_ERR,"%d is an invalid record length\n",
                    recordlength);
                serrno = EINVAL;
                rc = -1;
                break;
            }
            filereq->recordlength = recordlength;
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}
static int rtcpc_n_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    if ( rc != -1 ) {
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            /*
             * Set switch
             */
            if ( filereq->check_fid == CHECK_FILE ) {
                rtcp_log(LOG_ERR,"ONLY ONE OF -n AND -o CAN BE SPECIFIED\n");
                serrno = EINVAL;
                rc = -1;
                break;
            }
            if ( filereq->check_fid == -1 )
                filereq->check_fid = NEW_FILE;
        } CLIST_ITERATE_END(tl->file,fl);
    } /* if ( rc != -1 )  */
    return(rc);
}

static int rtcpc_N_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    unsigned long maxnbrec = 0;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set option
        */
        if ( filereq->maxnbrec == 0 ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            maxnbrec = strtoul(p,NULL,10);
            filereq->maxnbrec = (u_signed64)maxnbrec;
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}

static int rtcpc_o_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set switch
        */
        if ( filereq->check_fid == NEW_FILE ) {
            rtcp_log(LOG_ERR,"ONLY ONE OF -n AND -o CAN BE SPECIFIED\n");
            serrno = EINVAL;
            rc = -1;
            break;
        }
        if ( filereq->check_fid == -1 ) 
            filereq->check_fid = CHECK_FILE;
    } CLIST_ITERATE_END(tl->file,fl);
    return(rc);
}

/*
 * Construct a comma-delimited list of tape file sequence numbers 
 * from the -q option value.
 */
static int rtcpc_expand_fseq(int mode, const char *q_opt, char **fseq_list) {
    int len;
    char *tmp,*p, *q_list;
    int i,j,k;
    int trailing = 0;
    char q_default[] = "1";
    
    if ( q_opt == NULL || fseq_list == NULL ) return(0);
    *fseq_list = q_list = NULL;
    
    if ( *q_opt == 'n' || *q_opt == 'u' ) {
        /*
         * For special values "nx" (append) and "ux" (position by
         * FID) the value is simply repeated "x" times in the output 
         * list.
         */
        if ( q_opt[1] != '\0' ) 
            i = atoi(&q_opt[1]);
        else
            i = 1;
        if ( (mode == WRITE_ENABLE) && (*q_opt == 'u') && (i>1) ) {
            rtcp_log(LOG_ERR,"Position by multiple FID is not valid for tape write\n");
            return(-1);
        }

        len = i*2+1;
        q_list = (char *)malloc(len);
        if ( q_list == NULL ) {
            serrno = errno;
            rtcp_log(LOG_ERR,"malloc(%d): %s\n",len,sstrerror(errno));
            return(-1);
        }
        memset(q_list,'\0',len);
        for (j=0; j<i; j++) {
            if ( *q_opt == 'n' ) {
                if ( mode == WRITE_DISABLE ) {
                    rtcp_log(LOG_ERR,"-qn option invalid for cptpdsk\n");
                    serrno = EINVAL;
                    free(q_list);
                    q_list = NULL;
                    return(-1);
                }
                strcat(q_list,",n");
            } else if ( *q_opt == 'u' ) strcat(q_list,",u");
        }
        *fseq_list = q_list;
        return(len);
    }
 
    tmp = (char *)malloc(strlen(q_opt)+1);
    if ( tmp == NULL ) {
        serrno = errno;
        rtcp_log(LOG_ERR,"malloc(%d): %s\n",strlen(q_opt)+1,
            sstrerror(errno));
        return(-1);
    }
    for (;;) {
        strcpy(tmp,q_opt);
        /*
         * Check for trailing "-" meaning that the remaining files
         * to end of tape (tpread only). This entry will be marked 
         * with a zero "0" in the output list.
         */
        if ( tmp[strlen(tmp)-1] == '-' ) {
            if ( mode == WRITE_ENABLE ) {
                rtcp_log(LOG_ERR,"trailing minus in -q sequence list is invalid for cpdsktp\n");
                serrno = EINVAL;
                if ( q_list != NULL ) free(q_list);
                q_list = NULL;
                free(tmp);
                return(-1);
            }
            tmp[strlen(tmp)-1] = '\0';
            trailing = 1;
        }
        p = strtok(tmp,",");
        if ( p == NULL || *p == '\0' ) p = (char *)&q_default[0];
        len = 0;
        while ( p != NULL ) {
            if ( strstr(p,"-") ) {
                i = j = 0;
                sscanf(p,"%d-%d",&i,&j);
                if ( i == 0 || j == 0 || j < i ) {
                    rtcp_log(LOG_ERR,"the file sequence list is not valid\n");
                    serrno = EINVAL;
                    free(tmp);
                    if ( q_list != NULL ) free(q_list);
                    q_list = NULL;
                    return(-1);
                }
                for (k=i; k<=j; k++) {
                    int i1,i2;
                    i2=0;
                    for (i1=1; i1<=k; i1*=10) i2++;
                    len += i2+1; /* length of k + extra space for "," */
                    if ( q_list != NULL )
                        sprintf(&q_list[strlen(q_list)],",%d",k);
                }
            } else {
                len += strlen(p)+1;
                if ( q_list != NULL ) 
                    sprintf(&q_list[strlen(q_list)],",%s",p);
            }
            p = strtok(NULL,",");
        }
        len++;
        if ( trailing ) len += 2;
        if ( q_list == NULL ) {
            q_list = (char *)malloc(len);
            if ( q_list == NULL ) {
                serrno = errno;
                free(tmp);
                rtcp_log(LOG_ERR,"malloc(%d): %s\n",len,sstrerror(errno));
                return(-1);
            }
            memset(q_list,'\0',len);
        } else break;
    }
    
    free(tmp);
    if ( trailing == 1 ) strcat(q_list,",0");
    *fseq_list = q_list;
    return(len);
}

static int rtcpc_q_opt(int mode,
                       const char *value,
                       tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char *fseq_list, *p, *q;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    fseq_list = NULL;
    rc = rtcpc_expand_fseq(mode,value,&fseq_list);
    if ( rc == -1 || fseq_list == NULL ) return(-1);
    p = q = fseq_list;
    if ( *p == ',' ) p++;

    if ( fseq_list == NULL ) rc = -1;

    rc = 0;
    tl = *tape;

    if ( tl->file == NULL ) rc = newFileList(tape,NULL,mode);

    tapereq = &tl->tapereq;

    while ( rc != -1 && q != NULL ) {
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            /*
             * Set position method and file sequence number for
             * this tape file.
             */
            if ( filereq->position_method == -1 ) {
                if ( (q = strstr(p,",")) != NULL ) *q = '\0';
                if ( *p == 'n' ) 
                    filereq->position_method = TPPOSIT_EOI;
                else if ( *p == 'u' ) 
                    filereq->position_method = TPPOSIT_FID;
                else {
                    filereq->position_method = TPPOSIT_FSEQ;
                    filereq->tape_fseq = atoi(p);
                }
                
                if ( q != NULL ) {
                    p = q;
                    p++;
                    if ( fl->next == tl->file &&
                        tl->next == *tape ) {
                        rc = newFileList(tape,NULL,mode);
                        if ( rc == -1 ) break;
                    }
                }
            }
        } CLIST_ITERATE_END(tl->file,fl);
        if ( q != NULL ) rc = newFileList(tape,NULL,mode);
    } /* while ( rc != -1 && q != NULL ) */

    if ( fseq_list != NULL ) free(fseq_list);
    return(rc);
}

static int rtcpc_s_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    unsigned long maxsizeMB = 0;
    u_signed64 maxsize = 0;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set option
        */
        if ( filereq->maxsize == 0 ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            maxsizeMB = strtoul(p,NULL,10);
            maxsize = (u_signed64)maxsizeMB * 1024 * 1024;
            filereq->maxsize = maxsize;
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}

static int rtcpc_S_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    char tmp[CA_MAXHOSTNAMELEN+1];
    tape_list_t *tl;
    rtcpTapeRequest_t *tapereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;

    if ( rc != -1 ) {
        /*
         * Set option
         */
        if ( strlen(value) > CA_MAXHOSTNAMELEN ) {
            tmp[sizeof(tmp)-1] = '\0';
            rtcp_log(LOG_ERR,"INVALID HOST NAME %s... is too long\n",
                     strncpy(tmp,value,sizeof(tmp)-1));
            serrno = EINVAL;
            rc = -1;
        }

        if ( rc != -1 ) strcpy(tapereq->server,value);
    }
    return(rc);
}

static int rtcpc_t_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    int retention = 0;
    char *p, *q, *local_value;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set option
        */
        if ( filereq->retention == -1 ) {
            if ( (q = strstr(p,":")) != NULL ) *q = '\0';
            retention = strtoul(p,NULL,10);
            filereq->retention = retention;
            if ( q != NULL ) {
                p = q;
                p++;
            }
        }
    } CLIST_ITERATE_END(tl->file,fl);
    free(local_value);
    return(rc);
}
static int rtcpc_T_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set switch
        */
        if ( filereq->tp_err_action == -1 ) filereq->tp_err_action = 0;
        filereq->tp_err_action |= NOTRLCHK;
    } CLIST_ITERATE_END(tl->file,fl);
    return(rc);
}
static int rtcpc_v_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc, last_tape_fseq;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    char *local_value, *p, *q, tmp[CA_MAXVSNLEN+1];

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;
    
    rc = 0;

    while ( rc != -1 && q != NULL ) {
        CLIST_ITERATE_BEGIN(*tape,tl) {
            if ( tl->file == NULL ) {
                rc = newFileList(&tl,NULL,mode);
                if ( rc != -1 ) {
                    /*
                     * Last tape file may spann over several volumes
                     */
                    if ( tl != *tape && tl->prev->file != NULL ) {
                        last_tape_fseq = tl->prev->file->prev->filereq.tape_fseq;
                        CLIST_ITERATE_BEGIN(tl->prev->file,fl) {
                            if ( fl->filereq.tape_fseq == last_tape_fseq ) { 
                                tl->file->prev->filereq.tape_fseq = 
                                    last_tape_fseq;
                                /*
                                 * We did already create a first element
                                 * above.
                                 */
                                if ( fl != tl->prev->file->prev )
                                    rc = newFileList(&tl,NULL,mode);
                            }
                        } CLIST_ITERATE_END(tl->prev->file,fl);
                    }
                }
            }
            tapereq = &tl->tapereq;

            if ( *tapereq->vsn == '\0' ) {
                if ( (q = strstr(p,":")) != NULL ) *q = '\0';
                if ( strlen(p) > CA_MAXVSNLEN ) {
                    tmp[sizeof(tmp)-1] = '\0';
                    rtcp_log(LOG_ERR,"INVALID VSN SPECIFIED %s... is too long\n",
                             strncpy(tmp,p,sizeof(tmp)-1));
                    serrno = EINVAL;
                    rc = -1;
                    break;
                }
                strcpy(tapereq->vsn,p);

                if ( q != NULL ) {
                    p = q;
                    p++;
                    if ( tl->file->next == tl->file &&
                        tl->next == *tape ) {
                        rc = newTapeList(tape,NULL,mode);
                        if ( rc == -1 ) break;
                    }
                }
            }
        } CLIST_ITERATE_END(*tape,tl);
        if ( q != NULL ) rc = newTapeList(tape,NULL,mode);
    } /* while (rc != -1 && q != NULL ) */
    free(local_value);
    return(rc);
}

static int rtcpc_V_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc, last_tape_fseq;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    char *local_value, *p, *q, tmp[CA_MAXVIDLEN+1];

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    local_value = (char *)malloc(strlen(value)+1);
    if ( local_value == NULL ) {
        serrno = errno;
        return(-1);
    }
    strcpy(local_value,value);
    q = p = local_value;

    rc = 0;

    while ( rc != -1 && q != NULL ) {
        CLIST_ITERATE_BEGIN(*tape,tl) {
            if ( tl->file == NULL ) {
                rc = newFileList(&tl,NULL,mode);
                if ( rc != -1 ) {
                    /*
                     * Last tape file may spann over several volumes
                     */
                    if ( tl != *tape && tl->prev->file != NULL ) {
                        last_tape_fseq = tl->prev->file->prev->filereq.tape_fseq;
                        CLIST_ITERATE_BEGIN(tl->prev->file,fl) {
                            if ( fl->filereq.tape_fseq == last_tape_fseq ) { 
                                tl->file->prev->filereq.tape_fseq = 
                                    last_tape_fseq;
                                /*
                                 * We did already create a first element
                                 * above.
                                 */
                                if ( fl != tl->prev->file->prev ) 
                                    rc = newFileList(&tl,NULL,mode);
                            }
                        } CLIST_ITERATE_END(tl->prev->file,fl);
                    }
                }
            }
            tapereq = &tl->tapereq;

            if ( *tapereq->vid == '\0' ) {
                if ( (q = strstr(p,":")) != NULL ) *q = '\0';
                if ( q == NULL && tl->prev != tl && 
                     strcmp(tl->prev->tapereq.vid,p) == 0 ) {
                        strcpy(tapereq->vid,tapereq->vsn);    
                } else {
                    if ( strlen(p) > CA_MAXVIDLEN ) {
                        tmp[sizeof(tmp)-1] = '\0';
                        rtcp_log(LOG_ERR,"INVALID VID SPECIFIED %s... is too long\n",
                                 strncpy(tmp,p,sizeof(tmp)-1));
                        serrno = EINVAL;
                        rc = -1;
                        break;
                    }
                    strcpy(tapereq->vid,p);
                }

                if ( q != NULL ) {
                    p = q;
                    p++;
                    if ( tl->file->next == tl->file &&
                        tl->next == *tape ) {
                        rc = newTapeList(tape,NULL,mode);
                        if ( rc == -1 ) break;
                    }
                }
            }
        } CLIST_ITERATE_END(*tape,tl);
        if ( q != NULL ) rc = newTapeList(tape,NULL,mode);
    } /* while (rc != -1 && q != NULL ) */
    free(local_value);
    return(rc);
}
static int rtcpc_x_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc = 0;
    /*
     * Set LOG_DEBUG (7) loglevel
     */
    putenv("RTCOPY_LOGLEVEL=7");
    return(rc);
}
/*
 * VMS lega stuff. Must be continued to be supported until
 * we got rid of all old clients (and hence VMS).
 */
static int rtcpc_X_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    return(0);
}

static int rtcpc_Z_opt(int mode, 
                     const char *value, 
                     tape_list_t **tape) {
    int rc;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( value == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }

    rc = 0;
    tl = *tape;

    tapereq = &tl->tapereq;
    
    CLIST_ITERATE_BEGIN(tl->file,fl) {
        filereq = &fl->filereq;
        /*
        * Set option if not already set
        */
        if ( *filereq->stageID == '\0' )
            strcpy(filereq->stageID,value);
    } CLIST_ITERATE_END(tl->file,fl);
    return(rc);
}

/*
 * Process disk files in backward order to assure that input option
 * files containing disk paths are correctly processed.
 */
static int rtcpc_diskfiles(int mode,
                           const char *filename,
                           tape_list_t **tape) {
    int rc, toomany, disk_fseq;
    tape_list_t *tl, *tl1;
    file_list_t *fl, *fl1;
    rtcpFileRequest_t *filereq;
    char *last_filename, tmp[CA_MAXPATHLEN+1];

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( filename != NULL && (*filename == '\0' || 
        strlen(filename) > CA_MAXPATHLEN) ) {
        if ( strlen(filename) > CA_MAXPATHLEN ) {
            tmp[sizeof(tmp)-1] = '\0';
            rtcp_log(LOG_ERR,"INVALID FILE PATH %s... is too long\n",strncpy(tmp,filename,sizeof(tmp)-1));
            serrno = SENAMETOOLONG;
        } else  {
            rtcp_log(LOG_ERR,"INVALID FILE PATH %s\n",filename);
            serrno = EINVAL;
        }
        return(-1);
    }

    rc = 0;
    tl = *tape;

    if ( filename != NULL ) {
        if ( tl == NULL || tl->file == NULL ) {
            /*
             * -q option was not specified. Default is -q 1
             */
            rc = newFileList(tape,&fl,mode);
            if ( fl != NULL ) {
                fl->filereq.tape_fseq = 1;
                fl->filereq.position_method = TPPOSIT_FSEQ;
            }
            tl = *tape;
        } 
        toomany = 1;
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            if ( ((filereq->tape_fseq > 0) || 
                  (filereq->position_method != TPPOSIT_FSEQ)) && 
                 (*filereq->file_path == '\0') ) {
                strcpy(filereq->file_path,filename);
                /*
                 * If same file name is repeated it is *not* a
                 * concatenation. The user explicitly want's to overwrite
                 * the same file (useful for perf. tests to /dev/null).
                 */
                if ( strcmp(fl->prev->filereq.file_path,filename) == 0 )
                    filereq->concat = NOCONCAT;
                toomany = 0;
                break;
            }
        } CLIST_ITERATE_END(tl->file,fl);
        if ( (filereq != NULL) && (toomany != 0) ) {
            if ( (fl == tl->file) && 
                 (fl->prev->filereq.tape_fseq == 0) &&
                 (filereq->position_method == TPPOSIT_FSEQ) &&
                 (mode == WRITE_DISABLE) ) {
                /*
                 * There are more disk files than tape files. This is OK
                 * on tape read if there was a trailing minus ("-") in the
                 * tape file sequence list specified with the -q option.
                 * In this case the tape file sequence number of the last
                 * file list element is set to zero (0). This last
                 * element is just a place holder to indicate that there
                 * was a trailing minus. It should be kept while processing
                 * the disk files and chopped off when all options and
                 * disk files have been processed.
                 */
                rc = newFileList(tape,&fl,mode);
                if ( rc != -1 ) {
                    /*
                     * Copy the previous last element (place holder).
                     */
                    fl->filereq = fl->prev->filereq;
                    fl = fl->prev;
                    filereq = &fl->filereq;
                    filereq->tape_fseq = fl->prev->filereq.tape_fseq + 1;
                    filereq->concat = NOCONCAT_TO_EOD;
                    strcpy(filereq->file_path,filename);
                    toomany = 0;
                } 
            } else if ( (fl == tl->file) && 
                        (((fl->prev->filereq.tape_fseq > 0) &&
                          (filereq->position_method == TPPOSIT_FSEQ)) ||
                         (filereq->position_method == TPPOSIT_EOI)) &&
                       (mode == WRITE_ENABLE) ) {
                /* 
                 * There are more disk files than tape files and there
                 * was *no* trailing minus ("-") in the tape file sequence
                 * list specified with the -q option. This is OK for
                 * tape write where all remaining disk files will
                 * be concatenated to the last tape file.
                 */
                rc = newFileList(tape,&fl,mode);
                if ( rc != -1 ) {
                    fl->filereq = fl->prev->filereq;
                    fl->filereq.concat = CONCAT;
                    strcpy(fl->filereq.file_path,filename);
                    toomany = 0;
                } 
            } else {
                rtcp_log(LOG_ERR,"incorrect number of filenames specified\n");
                serrno = EINVAL;
                rc = -1;
            }
        } 
    } else { 
        last_filename = NULL;
        disk_fseq = 0;
        if ( tl == NULL || tl->file == NULL ) return(0);
        fl = tl->file->prev;
        if ( fl->filereq.tape_fseq == 0 ) {
            /*
             * There was a trailing minus ("-"). Set CONCAT to EOD for
             * last specified file and remove the place holder element.
             * Only valid for tape read!
             */
            if ( tl->tapereq.mode == WRITE_ENABLE ) {
                /*
                 * This should never happen! the check is already done in
                 * rtcp_q_opt()
                 */
                rtcp_log(LOG_ERR,"trailing minus in -q sequence list is invalid for cpdsktp\n");
                serrno = EINVAL;
                return(-1);
            }
            /*
             * CONCAT_TO_EOD implies concatenation starting from the last
             * specified tape file (e.g. from tape file 3 if -q 1,3-). However,
             * we may very well already be concatenating from a previously
             * specified tape file, e.g.
             * tpread -V AA0000 -q 1,4,7- disk_file1 disk_file2
             * where the concatenation starts with tape file 4.
             * We cannot therefore not override CONCAT for the last file 
             * element. The concat flag for the three file elements of the
             * above request should be:
             * NOCONCAT, NOCONCAT, CONCAT|CONCAT_TO_EOD
             */
            if ( fl->prev->filereq.concat == -1 ||
                 (fl->prev->filereq.concat & CONCAT) == 0 )
                fl->prev->filereq.concat = CONCAT_TO_EOD;
            else
                fl->prev->filereq.concat |= CONCAT_TO_EOD;

            CLIST_DELETE(tl->file,fl);
            free(fl);
            fl = tl->file->prev;
            /*
             * If multi-volume request we must make sure that the file
             * elements on the other volumes are updated as well
             */
            CLIST_ITERATE_BEGIN(tl,tl1) {
                if ( tl1 != tl ) {
                    CLIST_ITERATE_BEGIN(tl1->file,fl1) {
                        if ( fl1->filereq.tape_fseq == 0 ) {
                            fl1->filereq = fl->filereq;
                        }
                    } CLIST_ITERATE_END(tl1->file,fl1);
                }
            } CLIST_ITERATE_END(tl,tl1);
        }
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            if ( *filereq->file_path == '\0' ) {
                if ( last_filename == NULL ) {
                    rtcp_log(LOG_ERR,
                        "Disk file pathnames must be specified\n");
                    serrno = EINVAL;
                    rc = -1;    
                    break;
                }
                /*
                 * More tape files than disk files. Valid for
                 * tape read where the remaining tape files then
                 * will be concatenated to the last disk file
                 */
                if ( mode == WRITE_ENABLE ) {
                    rtcp_log(LOG_ERR,"incorrect number of filenames specified\n");
                    serrno = EINVAL;
                    rc = -1;
                    break;
                }
                /*
                 * Both CONCAT and CONCAT_TO_EOD are allowed for
                 * the last file element (see discussion above).
                 */ 
                if ( filereq->concat == -1 ) filereq->concat = CONCAT;
                else if ( (filereq->concat & CONCAT_TO_EOD) != 0 )
                    filereq->concat |= CONCAT;
                strcpy(filereq->file_path,last_filename);
            } else {
                if ( last_filename != NULL &&
                     mode == WRITE_DISABLE &&
                     strcmp(filereq->file_path,".") != 0 &&
                     strcmp(last_filename,filereq->file_path) == 0 ) {
                    /*
                     * Concatenate (maybe to EOD). Don't override
                     * NOCONCAT if same file name has been repeated on the
                     * command line (e.g. tpread ... -q 1-2 crap crap) in
                     * which case we actually don't concatenate but
                     * overwrite.
                     */
                    if ( filereq->concat == -1 ) filereq->concat = CONCAT;
                    if ( (filereq->concat & (NOCONCAT|NOCONCAT_TO_EOD)) != 0 )
                        disk_fseq++;
                } else {
                    /*
                     * Don't overwrite previous set NOCONCAT_TO_EOD since
                     * the distinction is needed by server to determine the
                     * error severity when there are less files on tape than
                     * the number of specified disk files
                     */
                    if ( filereq->concat == -1 ) filereq->concat = NOCONCAT;
                    disk_fseq++;
                }
                last_filename = filereq->file_path;
            }
            filereq->disk_fseq = disk_fseq;
        } CLIST_ITERATE_END(tl->file,fl);
        /*
         * If multi-volume, copy file elements for the last tape file from
         * first volume to the other volumes.
         */
        CLIST_ITERATE_BEGIN(*tape,tl) {
            if ( tl != *tape ) {
                fl1 = tl->file;
                CLIST_ITERATE_BEGIN((*tape)->file,fl) {
                    if ( fl->filereq.tape_fseq == fl1->filereq.tape_fseq ) {
                        fl->filereq.concat |= VOLUME_SPANNING;
                        fl1->filereq = fl->filereq;
                        fl1 = fl1->next;
                    }
                } CLIST_ITERATE_END((*tape)->file,fl);
            }
        } CLIST_ITERATE_END(*tape,tl);
    }

    return(rc);
}

#define DMPTP_INT_OPT(X,Y) { \
    if ( (X) < 0 ) { \
        X = strtol(optarg,&dp,10); \
        if ( dp == NULL || *dp != '\0' ) { \
            rtcp_log(LOG_ERR, TP006, #Y); \
            errflg++; \
        } \
    } else { \
        rtcp_log(LOG_ERR, TP018, #Y); \
        errflg++; \
    }}


#define DMPTP_STR_OPT(X,Y) { \
    if ( *(X) == '\0' ) { \
        if ( strlen(optarg) < sizeof((X)) ) strcpy((X),optarg); \
        else { \
            rtcp_log(LOG_ERR, TP006, #Y); \
            errflg++; \
        } \
    } else { \
        rtcp_log(LOG_ERR, TP018, #Y); \
        errflg++; \
    }}

int rtcpc_InitDumpTapeReq(rtcpDumpTapeRequest_t *dump) {
    if ( dump == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    memset(dump,'\0',sizeof(rtcpDumpTapeRequest_t));
    dump->maxbyte = -1;
    dump->blocksize = -1;
    dump->convert = -1;
    dump->tp_err_action = -1;
    dump->startfile = -1;
    dump->maxfile = -1;
    dump->fromblock = -1;
    dump->toblock = -1; 
    return(0);
}

int rtcpc_BuildDumpTapeReq(tape_list_t **tape,
                           int argc, char *argv[]) {
    rtcpDumpTapeRequest_t *dumpreq;
    rtcpTapeRequest_t *tapereq;
    int rc,c;
    char *p, *dp;
    int errflg = 0;
    extern int getopt();

    if ( tape == NULL || argc < 0 || argv == NULL ) {
        rtcp_log(LOG_ERR,"rtcpc_BuildDumpTapeReq() called with NULL args\n");
        serrno = EINVAL;
        return(-1);
    }
    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,WRITE_DISABLE);
        if ( rc == -1 ) return(-1);
    }
    tapereq = &(*tape)->tapereq;
    dumpreq = &(*tape)->dumpreq;

    rc = rtcpc_InitDumpTapeReq(dumpreq);
    if ( rc == -1 ) return(rc);

    while ( (c = getopt(argc,argv,"B:b:C:d:E:F:g:N:q:S:T:V:v:")) != EOF) {
        switch (c) {
        case 'B':
            DMPTP_INT_OPT(dumpreq->maxbyte,-B);
            break;
        case 'b':
            DMPTP_INT_OPT(dumpreq->blocksize,-b);
            break;
        case 'C':
            if ( dumpreq->convert < 0 ) {
                if (strcmp (optarg, "ascii") == 0 ) dumpreq->convert = ASCCONV;
                else if ( strcmp (optarg, "ebcdic") == 0 ) 
                    dumpreq->convert = EBCCONV;
                else {
                    rtcp_log(LOG_ERR, TP006, "-C");
                    errflg++;
                }
            } else {
                rtcp_log(LOG_ERR, TP018, "-C");
                errflg++;
            }
            break;
        case 'd':
            DMPTP_STR_OPT(tapereq->density,-d);
            break;
        case 'E':
            if ( strcmp (optarg, "ignoreeoi") == 0 ) 
                dumpreq->tp_err_action = IGNOREEOI;
            else {
                rtcp_log(LOG_ERR, TP006, "-E");
                errflg++;
            }
            break;
        case 'F':
            DMPTP_INT_OPT(dumpreq->maxfile,-F);
            break;
        case 'g':
            DMPTP_STR_OPT(tapereq->dgn,-g);
            break;
        case 'N':
            if ( dumpreq->fromblock < 0 ) {
                if ( (p = strchr (optarg, ',')) != NULL ) {
                    *p++ = '\0';
                    dumpreq->fromblock = strtol(optarg, &dp, 10);
                    if ( dp == NULL || *dp != '\0' ) {
                        rtcp_log(LOG_ERR, TP006, "-N");
                        errflg++;
                    }
                } else {
                    p = optarg;
                    dumpreq->fromblock = 1;
                }
                dumpreq->toblock = strtol(p, &dp, 10);
                if ( dp == NULL || *dp != '\0' ) {
                    rtcp_log(LOG_ERR, TP006, "-N");
                    errflg++;
                }
            }
            break;
        case 'q':
            DMPTP_INT_OPT(dumpreq->startfile,-q);
            break;
        case 'S':
            DMPTP_STR_OPT(tapereq->server,-S);
            break;
        case 'V':
            DMPTP_STR_OPT(tapereq->vid,-V);
            break;
        case 'v':
            DMPTP_STR_OPT(tapereq->vsn,-v);
            break;
        case '?':
            errflg++;
            break;
        }
    }

    if ( *tapereq->vid == '\0' && *tapereq->vsn == '\0' ) {
        rtcp_log(LOG_ERR,TP031);
        errflg++;
    }
    if ( errflg > 0 ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( strcmp(tapereq->dgn,"CT1") == 0 ) strcpy(tapereq->dgn,"CART");
    return(0);
}

#define DUMPSTR(Y,X) {if ( *X != '\0' ) rtcp_log(LOG_DEBUG,"%s%s: %s\n",Y,#X,X);}
#define DUMPINT(Y,X) {if ( X != -1 ) rtcp_log(LOG_DEBUG,"%s%s: %d\n",Y,#X,X);}
#define DUMPULONG(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %lu\n",Y,#X,X);}
#define DUMPBLKID(Y,X) {rtcp_log(LOG_DEBUG,"%s%s: %d:%d:%d:%d\n",Y,#X,(int)X[0],(int)X[1],(int)X[2],(int)X[3]);}
#define DUMPUUID(Y,X) {char *__p; rtcp_log(LOG_DEBUG,"%s%s: %s\n",Y,#X,((__p = CuuidToString(X)) == NULL ? "(null)" : __p));if ( __p != NULL ) free(__p);} 
#define DUMPHEX(Y,X) {if ( X != -1 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%x\n",Y,#X,X);}
#if defined(_WIN32)
#define DUMPI64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %I64u\n",Y,#X,(u_signed64)X);}
#define DUMPX64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%I64x\n",Y,#X,(u_signed64)X);}
#elif defined(__osf__) && defined(__alpha)
#define DUMPI64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %lu\n",Y,#X,(u_signed64)X);}
#define DUMPX64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%lx\n",Y,#X,(u_signed64)X);}
#else 
#define DUMPI64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %llu\n",Y,#X,(u_signed64)X);}
#define DUMPX64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%llx\n",Y,#X,(u_signed64)X);}
#endif
static char *CuuidToString(Cuuid_t *uuid) {
    return(rtcp_voidToString((void *)uuid,sizeof(Cuuid_t)));
}

int DLL_DECL dumpTapeReq(tape_list_t *tl) {
    rtcpTapeRequest_t *tapereq;
    char indent[] = " ";

    if (tl == NULL ) return(-1);
    tapereq = &tl->tapereq;

    rtcp_log(LOG_DEBUG,"\n%s---->Tape request\n",indent);
    DUMPSTR(indent,tapereq->vid);
    DUMPSTR(indent,tapereq->vsn);
    DUMPSTR(indent,tapereq->label);
    DUMPSTR(indent,tapereq->dgn);
    DUMPSTR(indent,tapereq->devtype);
    DUMPSTR(indent,tapereq->density);
    DUMPSTR(indent,tapereq->server);
    DUMPSTR(indent,tapereq->unit);
    DUMPINT(indent,tapereq->VolReqID);
    DUMPINT(indent,tapereq->jobID);
    DUMPINT(indent,tapereq->mode);
    DUMPINT(indent,tapereq->start_file);
    DUMPINT(indent,tapereq->end_file);
    DUMPINT(indent,tapereq->side);
    DUMPINT(indent,tapereq->tprc);
    DUMPINT(indent,tapereq->TStartRequest);
    DUMPINT(indent,tapereq->TEndRequest);
    DUMPINT(indent,tapereq->TStartRtcpd);
    DUMPINT(indent,tapereq->TStartMount);
    DUMPINT(indent,tapereq->TEndMount);
    DUMPINT(indent,tapereq->TStartUnmount);
    DUMPINT(indent,tapereq->TEndUnmount);
    DUMPUUID(indent,&(tapereq->rtcpReqId));

    DUMPSTR(indent,tapereq->err.errmsgtxt);
    DUMPHEX(indent,tapereq->err.severity);
    DUMPINT(indent,tapereq->err.errorcode);
    DUMPINT(indent,tapereq->err.max_tpretry);
    DUMPINT(indent,tapereq->err.max_cpretry);

    return(0);
}
int DLL_DECL dumpFileReq(file_list_t *fl) {
    rtcpFileRequest_t *filereq;
    char indent[] = "    ";

    if ( fl == NULL ) return(-1);
    filereq = &fl->filereq;

    rtcp_log(LOG_DEBUG,"\n%s---->File request\n",indent);
    DUMPSTR(indent,filereq->file_path);
    DUMPSTR(indent,filereq->tape_path);
    DUMPSTR(indent,filereq->recfm);
    DUMPSTR(indent,filereq->fid);
    DUMPSTR(indent,filereq->ifce);
    DUMPSTR(indent,filereq->stageID);
    DUMPINT(indent,filereq->stageSubreqID);
    DUMPINT(indent,filereq->VolReqID);
    DUMPINT(indent,filereq->jobID);
    DUMPINT(indent,filereq->position_method);
    DUMPINT(indent,filereq->tape_fseq);
    DUMPINT(indent,filereq->disk_fseq);
    DUMPINT(indent,filereq->blocksize);
    DUMPINT(indent,filereq->recordlength);
    DUMPINT(indent,filereq->retention);
    DUMPINT(indent,filereq->def_alloc);
    DUMPINT(indent,filereq->rtcp_err_action);
    DUMPINT(indent,filereq->tp_err_action);
    DUMPINT(indent,filereq->convert);
    DUMPINT(indent,filereq->check_fid);
    DUMPINT(indent,filereq->concat);

    DUMPINT(indent,filereq->proc_status);
    DUMPINT(indent,filereq->cprc);
    DUMPINT(indent,filereq->TStartPosition);
    DUMPINT(indent,filereq->TEndPosition);
    DUMPINT(indent,filereq->TStartTransferDisk);
    DUMPINT(indent,filereq->TEndTransferDisk);
    DUMPINT(indent,filereq->TStartTransferTape);
    DUMPINT(indent,filereq->TEndTransferTape);
    DUMPBLKID(indent,filereq->blockid);

    DUMPI64(indent,filereq->bytes_in);
    DUMPI64(indent,filereq->bytes_out);
    DUMPI64(indent,filereq->host_bytes);

    DUMPI64(indent,filereq->maxnbrec);
    DUMPI64(indent,filereq->maxsize);

    DUMPI64(indent,filereq->startsize);

    DUMPSTR(indent,filereq->castorSegAttr.nameServerHostName);
    DUMPSTR(indent,filereq->castorSegAttr.segmCksumAlgorithm);
    DUMPULONG(indent,filereq->castorSegAttr.segmCksum);
    DUMPI64(indent,filereq->castorSegAttr.castorFileId);

    DUMPUUID(indent,&(filereq->stgReqId));

    DUMPSTR(indent,filereq->err.errmsgtxt);
    DUMPHEX(indent,filereq->err.severity);
    DUMPINT(indent,filereq->err.errorcode);
    DUMPINT(indent,filereq->err.max_tpretry);
    DUMPINT(indent,filereq->err.max_cpretry);

    return(0);
}
