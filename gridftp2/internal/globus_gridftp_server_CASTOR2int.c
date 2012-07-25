/******************************************************************************
 *                  globus_gridftp_server_CASTOR2int.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 *
 *
 * @author Victor Kotlyar, Victor.Kotlyar@cern.ch
 *****************************************************************************/

#if defined(linux)
#define _LARGE_FILES
#endif

#include <sys/types.h>
#include <dirent.h>
#include <string.h>

#include "globus_gridftp_server.h"
#include "dsi_CASTOR2int.h"

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include <zlib.h>
#include <sys/xattr.h>

#define  CA_MAXCKSUMLEN 32
#define  CA_MAXCKSUMNAMELEN 15

static
globus_version_t local_version =
{
    0, /* major version number */
    1, /* minor version number */
    1157544130,
    0 /* branch ID */
};

/*
 *  utility function to make errors
 */
 
static
globus_result_t
globus_l_gfs_make_error(
		const char *                        msg)
{
	char *                              err_str;
	globus_result_t                     result;
	
	GlobusGFSName(globus_l_gfs_make_error);

	err_str = globus_common_create_string("%s error: %s", msg,  strerror(errno));
	result = GlobusGFSErrorGeneric(err_str);

	globus_free(err_str);
	return result;
}

/* fill the statbuf into globus_gfs_stat_t */
void fill_stat_array(globus_gfs_stat_t * filestat, struct stat64 statbuf, char *name)
{
	filestat->mode = statbuf.st_mode;;
	filestat->nlink = statbuf.st_nlink;
	filestat->uid = statbuf.st_uid;
	filestat->gid = statbuf.st_gid;
	filestat->size = statbuf.st_size;

	filestat->mtime = statbuf.st_mtime;
	filestat->atime = statbuf.st_atime;
	filestat->ctime = statbuf.st_ctime;

	filestat->dev = statbuf.st_dev;
	filestat->ino = statbuf.st_ino;
	filestat->name = strdup(name); 
}
/* free memory in stat_array from globus_gfs_stat_t->name */
void free_stat_array(globus_gfs_stat_t * filestat,int count)
{
	int i;
	for(i=0;i<count;i++) free(filestat[i].name);
}

/* free memory for the checksum list */
void free_checksum_list(checksum_block_list_t *checksum_list)
{
	checksum_block_list_t *             checksum_list_p;
    checksum_block_list_t *             checksum_list_pp;
    checksum_list_p=checksum_list;
        
    while(checksum_list_p->next!=NULL){
      checksum_list_pp=checksum_list_p->next;
      globus_free(checksum_list_p);
      checksum_list_p=checksum_list_pp;
    }
    globus_free(checksum_list_p);
}

// comparison of 2 checksum_block_list_t* on their offset for the use of qsort
int offsetComparison(const void *first, const void *second) {
  checksum_block_list_t** f = (checksum_block_list_t**)first;
  checksum_block_list_t** s = (checksum_block_list_t**)second;
  long long int diff = (*f)->offset - (*s)->offset;
  // Note that we cannot simply return diff as this function should return
  // an int and the cast for values not fitting in 32 bits may screw things
  if (0 == diff) return 0;
  if (diff > 0) return 1;
  return -1;
}


/* a replacement for zlib adler32_combine for SLC4  */
#define BASE 65521UL    /* largest prime smaller than 65536 */
#define MOD(a) a %= BASE

unsigned long  adler32_combine_(unsigned int adler1,
                                unsigned int adler2,
                                globus_off_t len2)
{
    unsigned int sum1;
    unsigned int sum2;
    unsigned int rem;

    /* the derivation of this formula is left as an exercise for the reader */
    rem = (unsigned int)(len2 % BASE);
    sum1 = adler1 & 0xffff;
    sum2 = rem * sum1;
    MOD(sum2);
    sum1 += (adler2 & 0xffff) + BASE - 1;
    sum2 += ((adler1 >> 16) & 0xffff) + ((adler2 >> 16) & 0xffff) + BASE - rem;
    if (sum1 >= BASE) sum1 -= BASE;
    if (sum1 >= BASE) sum1 -= BASE;
    if (sum2 >= (BASE << 1)) sum2 -= (BASE << 1);
    if (sum2 >= BASE) sum2 -= BASE;
    return sum1 | (sum2 << 16);
}

unsigned long  adler32_0chunks(len)
     unsigned int len;   
{
  return ((len%BASE) << 16) | 1;
}

/*************************************************************************
 *  start
 *  -----
 *  This function is called when a new session is initialized, ie a user 
 *  connectes to the server.  This hook gives the dsi an oppertunity to
 *  set internal state that will be threaded through to all other
 *  function calls associated with this session.  And an oppertunity to
 *  reject the user.
 *
 *  finished_info.info.session.session_arg should be set to an DSI
 *  defined data structure.  This pointer will be passed as the void *
 *  user_arg parameter to all other interface functions.
 * 
 *  NOTE: at nice wrapper function should exist that hides the details 
 *        of the finished_info structure, but it currently does not.  
 *        The DSI developer should jsut follow this template for now
 ************************************************************************/
static
void
globus_l_gfs_CASTOR2int_start(
    globus_gfs_operation_t              op,
    globus_gfs_session_info_t *         session_info)
{
    globus_l_gfs_CASTOR2int_handle_t *       CASTOR2int_handle;
    globus_gfs_finished_info_t          finished_info;
    char *                              p;
    char *                              func="globus_l_gfs_CASTOR2int_start";
    
    GlobusGFSName(globus_l_gfs_CASTOR2int_start);

    CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *)
        globus_malloc(sizeof(globus_l_gfs_CASTOR2int_handle_t));

        
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started, uid: %u, gid: %u\n",func, getuid(),getgid());
    globus_mutex_init(&CASTOR2int_handle->mutex,NULL);
    
    memset(&finished_info, '\0', sizeof(globus_gfs_finished_info_t));
    finished_info.type = GLOBUS_GFS_OP_SESSION_START;
    finished_info.result = GLOBUS_SUCCESS;
    finished_info.info.session.session_arg = CASTOR2int_handle;
    finished_info.info.session.username = session_info->username;
    finished_info.info.session.home_dir = NULL; /* if null we will go to HOME directory */

    CASTOR2int_handle->use_uuid=GLOBUS_FALSE;
    CASTOR2int_handle->uuid=NULL;
    CASTOR2int_handle->fullDestPath=NULL;
    CASTOR2int_handle->access_mode=NULL;
    if((CASTOR2int_handle->uuid=getenv("UUID"))!=NULL) {
	    if((CASTOR2int_handle->fullDestPath=getenv("FULLDESTPATH"))!=NULL) {
		    if((CASTOR2int_handle->access_mode=getenv("ACCESS_MODE"))!=NULL) {
		       CASTOR2int_handle->use_uuid=GLOBUS_TRUE;
		       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: uuid access is on: uuid=\"%s\" fullDestPath=\"%s\" mode=\"%s\"\n",
			                      func, CASTOR2int_handle->uuid,CASTOR2int_handle->fullDestPath,CASTOR2int_handle->access_mode);
		    }
	    } 
    }
    
    CASTOR2int_handle->checksum_list=NULL;
    CASTOR2int_handle->checksum_list_p=NULL;
    
    CASTOR2int_handle->useCksum=1;
    if ((p = getenv ("USE_CKSUM")) !=NULL) 
        if (0 == strcasecmp(p, "no")) CASTOR2int_handle->useCksum=0;     
    if (CASTOR2int_handle->useCksum) globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: USE_CKSUM=YES\n",func);
    else globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: USE_CKSUM=NO\n",func);
    
    globus_gridftp_server_operation_finished(op, GLOBUS_SUCCESS, &finished_info);
}

/*************************************************************************
 *  destroy
 *  -------
 *  This is called when a session ends, ie client quits or disconnects.
 *  The dsi should clean up all memory they associated wit the session
 *  here. 
 ************************************************************************/
static
void
globus_l_gfs_CASTOR2int_destroy(
    void *                              user_arg)
{
    globus_l_gfs_CASTOR2int_handle_t *       CASTOR2int_handle;

    CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
    
    globus_mutex_destroy(&CASTOR2int_handle->mutex);
    
    globus_free(CASTOR2int_handle);
}

/*************************************************************************
 *  stat
 *  ----
 *  This interface function is called whenever the server needs 
 *  information about a given file or resource.  It is called then an
 *  LIST is sent by the client, when the server needs to verify that 
 *  a file exists and has the proper permissions, etc.
 ************************************************************************/
static
void
globus_l_gfs_CASTOR2int_stat(
    globus_gfs_operation_t              op,
    globus_gfs_stat_info_t *            stat_info,
    void *                              user_arg)
{
    globus_gfs_stat_t *                 stat_array;
    int                                 stat_count;
    globus_l_gfs_CASTOR2int_handle_t *  CASTOR2int_handle;
    
    char *                              func="globus_l_gfs_CASTOR2int_stat";
    struct stat64                       statbuf;
    int                                 status=0;
    globus_result_t                     result;
    char *                              pathname;
    char *                              uuid_path;
    
    GlobusGFSName(globus_l_gfs_CASTOR2int_stat);

    CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
    
    if(CASTOR2int_handle->use_uuid) { /* we will use fullDestPath instead of client "path", and "path" must be in uuid form */
       uuid_path = stat_info->pathname;
       while(strchr(uuid_path, '/') != NULL) {
         /* strip any preceding path to isolate only the uuid part; see also CASTOR2int_handle_open */
         uuid_path = strchr(uuid_path, '/') + 1;
       }
       if(strcmp(uuid_path, CASTOR2int_handle->uuid) == 0)
         pathname = strdup(CASTOR2int_handle->fullDestPath);
       else
         pathname = strdup(func); /* we want that stat64 fails */
    }
    else
      pathname = strdup(func);
        
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s\n",func,pathname);
    
    status=stat64(pathname,&statbuf);
    if(status!=0) {
	    result=globus_l_gfs_make_error("fstat64");
	    globus_gridftp_server_finished_stat(op,result,NULL, 0);
	    free(pathname);
	    return;
    }
	
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: stat for the file: %s\n",func,pathname);
    stat_array = (globus_gfs_stat_t *) globus_calloc(1, sizeof(globus_gfs_stat_t));
    if(stat_array==NULL) {
       result=GlobusGFSErrorGeneric("error: memory allocation failed");
       globus_gridftp_server_finished_stat(op,result,NULL, 0);
       free(pathname);
       return;
    }
    stat_count=1;
    fill_stat_array(&(stat_array[0]),statbuf,pathname);
    globus_gridftp_server_finished_stat(op, GLOBUS_SUCCESS, stat_array, stat_count);
    free_stat_array(stat_array, stat_count);
    globus_free(stat_array);
    free(pathname);
    return;
}

/*************************************************************************
 *  command
 *  -------
 *  This interface function is called when the client sends a 'command'.
 *  commands are such things as mkdir, remdir, delete.  The complete
 *  enumeration is below.
 *
 *  To determine which command is being requested look at:
 *      cmd_info->command
 *
 *      GLOBUS_GFS_CMD_MKD = 1,
 *      GLOBUS_GFS_CMD_RMD,
 *      GLOBUS_GFS_CMD_DELE,
 *      GLOBUS_GFS_CMD_RNTO,
 *      GLOBUS_GFS_CMD_RNFR,
 *      GLOBUS_GFS_CMD_CKSM,
 *      GLOBUS_GFS_CMD_SITE_CHMOD,
 *      GLOBUS_GFS_CMD_SITE_DSI
 ************************************************************************/
static
void
globus_l_gfs_CASTOR2int_command(
    globus_gfs_operation_t              op,
    globus_gfs_command_info_t *         cmd_info,
    void *                              user_arg)
{
    globus_result_t                     result;
    (void)cmd_info;
    (void)user_arg;
    GlobusGFSName(globus_l_gfs_CASTOR2int_command);
    /* in gridftp disk server we do not allow to perform commads */
    result=GlobusGFSErrorGeneric("error: commands denied");
    globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
    return;
}
 
int CASTOR2int_handle_open(char *path, int flags, int mode, globus_l_gfs_CASTOR2int_handle_t * CASTOR2int_handle)
   {
   int   	rc;
   char *	func="CASTOR2int_handle_open";
   char * uuid_path;
   
   if(CASTOR2int_handle->use_uuid) { /* we will use fullDestPath instead of client "path", and "path" must be in uuid form */
     uuid_path = path;
     while(strchr(uuid_path, '/') != NULL) {
       /* strip any preceding path to isolate only the uuid part; see also globus_l_gfs_CASTOR2int_stat */
       uuid_path = strchr(uuid_path, '/') + 1;
     }
     if(strcmp(uuid_path,CASTOR2int_handle->uuid) == 0) {
       /* if clients uuid is the same as internal uuid we will access fullDestPath file then */
       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: open file in uuid mode \"%s\"\n",func,CASTOR2int_handle->fullDestPath);
       rc = open64 (CASTOR2int_handle->fullDestPath,flags,mode);
       return (rc);
	   }
	   globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: client and server uuids do not match in uuid mode \"%s\" != \"%s\"\n",
		      func, uuid_path,CASTOR2int_handle->uuid);
	   errno = ENOENT;
	   return (-1);
	 }
   errno = EINVAL;
   return (-1);
}

  /* receive from client */
static
void
globus_l_gfs_file_net_read_cb(
		globus_gfs_operation_t              op,
		globus_result_t                     result,
		globus_byte_t *                     buffer,
		globus_size_t                       nbytes,
		globus_off_t                        offset,
		globus_bool_t                       eof,
		void *                              user_arg)
{
	globus_off_t                        start_offset;
	globus_l_gfs_CASTOR2int_handle_t *     CASTOR2int_handle;
	globus_size_t                       bytes_written;
	unsigned long                       adler;
    checksum_block_list_t**             checksum_array;
    checksum_block_list_t *             checksum_list_pp;
    unsigned long                       index;
    unsigned long                       i;
    unsigned long                       file_checksum;
    char                                ckSumbuf[CA_MAXCKSUMLEN+1]; 
    char *                              ckSumalg="ADLER32"; /* we only support Adler32 for gridftp */
    char *                              func="globus_l_gfs_file_net_read_cb";
        
	CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
        
	globus_mutex_lock(&CASTOR2int_handle->mutex);
	{
		if(eof)	CASTOR2int_handle->done = GLOBUS_TRUE;
		CASTOR2int_handle->outstanding--;
		if(result != GLOBUS_SUCCESS) {
			CASTOR2int_handle->cached_res = result;
			CASTOR2int_handle->done = GLOBUS_TRUE;
		}
		else if(nbytes > 0) {
		  start_offset = lseek64(CASTOR2int_handle->fd, offset, SEEK_SET);
		  if(start_offset != offset) {
		    CASTOR2int_handle->cached_res = globus_l_gfs_make_error("seek");
		    CASTOR2int_handle->done = GLOBUS_TRUE;
		  } else {
		    bytes_written = write(CASTOR2int_handle->fd, buffer, nbytes);
		    if (CASTOR2int_handle->useCksum) {
		      /* fill the checksum list  */
		      /* we will have a lot of checksums blocks in the list */
		      adler = adler32(0L, Z_NULL, 0);
		      adler = adler32(adler, buffer, nbytes);
                    
		      CASTOR2int_handle->checksum_list_p->next=(checksum_block_list_t *)globus_malloc(sizeof(checksum_block_list_t));
                    
		      if (CASTOR2int_handle->checksum_list_p->next==NULL) {
                        CASTOR2int_handle->cached_res = GLOBUS_FAILURE;
                        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: malloc error \n",func);
                        CASTOR2int_handle->done = GLOBUS_TRUE;
                        globus_mutex_unlock(&CASTOR2int_handle->mutex);
                        return;
		      }
		      CASTOR2int_handle->checksum_list_p->next->next=NULL;
		      CASTOR2int_handle->checksum_list_p->offset=offset;
		      CASTOR2int_handle->checksum_list_p->size=bytes_written;
		      CASTOR2int_handle->checksum_list_p->csumvalue=adler;
		      CASTOR2int_handle->checksum_list_p=CASTOR2int_handle->checksum_list_p->next;
		      CASTOR2int_handle->number_of_blocks++;
		      /* end of the checksum section */
		    }
		    if(bytes_written < nbytes) {
		      errno = ENOSPC;
		      CASTOR2int_handle->cached_res = globus_l_gfs_make_error("write");
		      CASTOR2int_handle->done = GLOBUS_TRUE;
		      free_checksum_list(CASTOR2int_handle->checksum_list);
		    } else {
		      globus_gridftp_server_update_bytes_written(op,offset,nbytes);
		    }
		  }
		}

		globus_free(buffer);
		/* if not done just register the next one */
		if(!CASTOR2int_handle->done) globus_l_gfs_CASTOR2int_read_from_net(CASTOR2int_handle);
		/* if done and there are no outstanding callbacks finish */
		else if(CASTOR2int_handle->outstanding == 0){
		  if (CASTOR2int_handle->useCksum) {
		    /* checksum calculation */
		    checksum_array=(checksum_block_list_t**)globus_calloc(CASTOR2int_handle->number_of_blocks,sizeof(checksum_block_list_t*));
		    if (checksum_array==NULL){
		      free_checksum_list(CASTOR2int_handle->checksum_list);
		      CASTOR2int_handle->cached_res = GLOBUS_FAILURE;
		      globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: malloc error \n",func);
		      CASTOR2int_handle->done = GLOBUS_TRUE;
		      close(CASTOR2int_handle->fd);
		      globus_mutex_unlock(&CASTOR2int_handle->mutex);
		      return;
		    }
		    checksum_list_pp=CASTOR2int_handle->checksum_list;
		    /* sorting of the list to the array */
		    index = 0;
		    while (checksum_list_pp->next != NULL) { /* the latest block is always empty and has next pointer as NULL */
		      checksum_array[index] = checksum_list_pp;
		      checksum_list_pp=checksum_list_pp->next;
		      index++;
		    }
		    qsort(checksum_array, index, sizeof(checksum_block_list_t*), offsetComparison);
		    /* combine checksums, while making sure that we deal with missing chunks */
		    globus_off_t offset = 0;
		    /* check whether first chunk is missing */
		    if (checksum_array[0]->offset != 0) {
		      /* first chunk is missing. Consider it full of 0s */
		      offset = checksum_array[0]->offset;
		      file_checksum = adler32_combine_(adler32_0chunks(offset),
						       checksum_array[0]->csumvalue,
						       checksum_array[0]->size);
		    } else {
		      file_checksum = checksum_array[0]->csumvalue;
		    }
		    offset += checksum_array[0]->size;
		    /* go over all received chunks */
		    for (i=1;i<CASTOR2int_handle->number_of_blocks;i++) {
		      /* check the continuity with previous chunk */
		      if (checksum_array[i]->offset != offset) {
			// not continuous, a chunk is missing, consider it full of 0s
			globus_off_t doff = checksum_array[i]->offset - offset;
			file_checksum = adler32_combine_(file_checksum, adler32_0chunks(doff), doff);
			offset = checksum_array[i]->offset;
		      }
		      /* now handle the next chunk */
		      file_checksum=adler32_combine_(file_checksum,
						     checksum_array[i]->csumvalue,
						     checksum_array[i]->size);
		      offset += checksum_array[i]->size;
		    }

		    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: checksum for %s : AD 0x%lx\n",func,CASTOR2int_handle->fullDestPath,file_checksum);
		    globus_free(checksum_array);
		    free_checksum_list(CASTOR2int_handle->checksum_list);
		    /* set extended attributes */
		    sprintf(ckSumbuf,"%lx",file_checksum);
		    if (fsetxattr(CASTOR2int_handle->fd,"user.castor.checksum.type",ckSumalg,strlen(ckSumalg),0)) {
		      ;/* ignore all errors */
		    } else if (fsetxattr(CASTOR2int_handle->fd,"user.castor.checksum.value",ckSumbuf,strlen(ckSumbuf),0)) {
		      ; /* ignore all errors */
		    }
		    /* end of checksum section */
		  }
		  close(CASTOR2int_handle->fd);
			
		  globus_gridftp_server_finished_transfer(op, CASTOR2int_handle->cached_res);
		}
	}
	globus_mutex_unlock(&CASTOR2int_handle->mutex);
}
 
   
static
void
globus_l_gfs_CASTOR2int_read_from_net(
		globus_l_gfs_CASTOR2int_handle_t *         CASTOR2int_handle)
{
	globus_byte_t *                     buffer;
	globus_result_t                     result;
	char * 				    func="globus_l_gfs_CASTOR2int_read_from_net";
	
	GlobusGFSName(globus_l_gfs_CASTOR2int_read_from_net);
	
	/* in the read case this number will vary */
	globus_gridftp_server_get_optimal_concurrency(CASTOR2int_handle->op, &CASTOR2int_handle->optimal_count);
	
	while(CASTOR2int_handle->outstanding < CASTOR2int_handle->optimal_count) {
		buffer=globus_malloc(CASTOR2int_handle->block_size);
		if (buffer == NULL) {
			result = GlobusGFSErrorGeneric("error: globus malloc failed");
			CASTOR2int_handle->cached_res = result;
			CASTOR2int_handle->done = GLOBUS_TRUE;
			if(CASTOR2int_handle->outstanding == 0) {
				close(CASTOR2int_handle->fd);
				globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
			}
			return;
		}
		result= globus_gridftp_server_register_read(
				CASTOR2int_handle->op,
				buffer,
				CASTOR2int_handle->block_size,
				globus_l_gfs_file_net_read_cb,
				CASTOR2int_handle);
		
		if(result != GLOBUS_SUCCESS)  {
			globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: register read has finished with a bad result \n",func);
			globus_free(buffer);
			CASTOR2int_handle->cached_res = result;
			CASTOR2int_handle->done = GLOBUS_TRUE;
			if(CASTOR2int_handle->outstanding == 0) {
				close(CASTOR2int_handle->fd);
				globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
			}
			return;
		}
		CASTOR2int_handle->outstanding++;
	}
}

/*************************************************************************
 *  recv
 *  ----
 *  This interface function is called when the client requests that a
 *  file be transfered to the server.
 *
 *  To receive a file the following functions will be used in roughly
 *  the presented order.  They are doced in more detail with the
 *  gridftp server documentation.
 *
 *      globus_gridftp_server_begin_transfer();
 *      globus_gridftp_server_register_read();
 *      globus_gridftp_server_finished_transfer();
 *
 ************************************************************************/

static
void
globus_l_gfs_CASTOR2int_recv(
    globus_gfs_operation_t              op,
    globus_gfs_transfer_info_t *        transfer_info,
    void *                              user_arg)
{
    globus_l_gfs_CASTOR2int_handle_t *     CASTOR2int_handle;
    
    globus_result_t                     result;
    char * 				func="globus_l_gfs_CASTOR2int_recv";
    char * 				pathname;
    int 				flags;
    
    GlobusGFSName(globus_l_gfs_CASTOR2int_recv);
    CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
   
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);
    
    if(CASTOR2int_handle->use_uuid) /* we use uuid mode to access files */
       if( strcmp(CASTOR2int_handle->access_mode,"w") != 0 && strcmp(CASTOR2int_handle->access_mode,"o") !=0 ) {
	    result=GlobusGFSErrorGeneric("error: incorect access mode");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
       }
    pathname=strdup(transfer_info->pathname);
    if(pathname==NULL) {
	    result=GlobusGFSErrorGeneric("error: strdup failed");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s \n",func,pathname);
    
    /* try to open */
    flags = O_WRONLY | O_CREAT;
    if(transfer_info->truncate) flags |= O_TRUNC;
    
    CASTOR2int_handle->fd = CASTOR2int_handle_open(pathname, flags, 0644, CASTOR2int_handle);
    
    if(CASTOR2int_handle->fd < 0) {
	    result=globus_l_gfs_make_error("open/create");
	    free(pathname);
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
    }
	    
   /* reset all the needed variables in the handle */
   CASTOR2int_handle->cached_res = GLOBUS_SUCCESS;
   CASTOR2int_handle->outstanding = 0;
   CASTOR2int_handle->done = GLOBUS_FALSE;
   CASTOR2int_handle->blk_length = 0;
   CASTOR2int_handle->blk_offset = 0;
   CASTOR2int_handle->op = op;
   
   globus_gridftp_server_get_block_size(op, &CASTOR2int_handle->block_size);
   globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: block size: %ld\n",func,CASTOR2int_handle->block_size);
   
   /* here we will save all checksums for the file blocks        */
   /* malloc memory for the first element in the checksum list   */
   /* we should always have at least one block for a file        */
   CASTOR2int_handle->checksum_list=(checksum_block_list_t *)globus_malloc(sizeof(checksum_block_list_t));
   if (CASTOR2int_handle->checksum_list==NULL) {
         globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: malloc error \n",func);
         globus_gridftp_server_finished_transfer(op, GLOBUS_FAILURE);
	 return;
   }
   CASTOR2int_handle->checksum_list->next=NULL;
   CASTOR2int_handle->checksum_list_p=CASTOR2int_handle->checksum_list;
   CASTOR2int_handle->number_of_blocks=0;
   
   globus_gridftp_server_begin_transfer(op, 0, CASTOR2int_handle);
   
   globus_mutex_lock(&CASTOR2int_handle->mutex);
      {
      globus_l_gfs_CASTOR2int_read_from_net(CASTOR2int_handle);
      }
   globus_mutex_unlock(&CASTOR2int_handle->mutex);
   free(pathname); 
   globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
   return;
}

/*************************************************************************
 *  send
 *  ----
 *  This interface function is called when the client requests to receive
 *  a file from the server.
 *
 *  To send a file to the client the following functions will be used in roughly
 *  the presented order.  They are doced in more detail with the
 *  gridftp server documentation.
 *
 *      globus_gridftp_server_begin_transfer();
 *      globus_gridftp_server_register_write();
 *      globus_gridftp_server_finished_transfer();
 *
 ************************************************************************/
static
void
globus_l_gfs_CASTOR2int_send(
    globus_gfs_operation_t              op,
    globus_gfs_transfer_info_t *        transfer_info,
    void *                              user_arg)
{
    globus_l_gfs_CASTOR2int_handle_t *       CASTOR2int_handle;
    char * 				func="globus_l_gfs_CASTOR2int_send";
    char *				pathname;
    int 				i;
    globus_bool_t                       done;
    globus_result_t                     result;
    
    GlobusGFSName(globus_l_gfs_CASTOR2int_send);
    CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);
    
    if(CASTOR2int_handle->use_uuid) /* we use uuid mode to access files */
       if( strcmp(CASTOR2int_handle->access_mode,"r") != 0 && strcmp(CASTOR2int_handle->access_mode,"o") !=0 ) {
	    result=GlobusGFSErrorGeneric("error: incorect access mode");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
       }
    
    pathname=strdup(transfer_info->pathname);
    if (pathname == NULL) {
	result = GlobusGFSErrorGeneric("error: strdup failed");
	globus_gridftp_server_finished_transfer(op, result);
	return;
    }
    
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s\n",func,pathname);
    CASTOR2int_handle->fd = CASTOR2int_handle_open(pathname, O_RDONLY, 0, CASTOR2int_handle);  /* mode is ignored */
    
    if(CASTOR2int_handle->fd < 0) {
	result = globus_l_gfs_make_error("open");
	free(pathname);
	globus_gridftp_server_finished_transfer(op, result);
	return;
    }
    
    /* reset all the needed variables in the handle */
    CASTOR2int_handle->cached_res = GLOBUS_SUCCESS;
    CASTOR2int_handle->outstanding = 0;
    CASTOR2int_handle->done = GLOBUS_FALSE;
    CASTOR2int_handle->blk_length = 0;
    CASTOR2int_handle->blk_offset = 0;
    CASTOR2int_handle->op = op;
    
    globus_gridftp_server_get_optimal_concurrency(op, &CASTOR2int_handle->optimal_count);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: optimal_concurrency: %u\n",func,CASTOR2int_handle->optimal_count);
    
    globus_gridftp_server_get_block_size(op, &CASTOR2int_handle->block_size);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: block_size: %ld\n",func,CASTOR2int_handle->block_size);
    
    /* here we will save all checksums for the file blocks        */
    /* malloc memory for the first element in the checksum list   */
    /* we should always have at least one block for a file        */
    CASTOR2int_handle->checksum_list=(checksum_block_list_t *)globus_malloc(sizeof(checksum_block_list_t));
    if (CASTOR2int_handle->checksum_list==NULL) {
         globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: malloc error \n",func);
         globus_gridftp_server_finished_transfer(op, GLOBUS_FAILURE);
     return;
    }
    CASTOR2int_handle->checksum_list->next=NULL;
    CASTOR2int_handle->checksum_list_p=CASTOR2int_handle->checksum_list;
    CASTOR2int_handle->number_of_blocks=0;
    
    globus_gridftp_server_begin_transfer(op, 0, CASTOR2int_handle);
    done = GLOBUS_FALSE;
    globus_mutex_lock(&CASTOR2int_handle->mutex);
    {
	   for(i = 0; i < CASTOR2int_handle->optimal_count && !done; i++) {
		done = globus_l_gfs_CASTOR2int_send_next_to_client(CASTOR2int_handle);
	   }
    }
    globus_mutex_unlock(&CASTOR2int_handle->mutex);
    free(pathname);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
}

static
globus_bool_t
globus_l_gfs_CASTOR2int_send_next_to_client(
		globus_l_gfs_CASTOR2int_handle_t *         CASTOR2int_handle)
{
	globus_result_t                     result;
	globus_result_t                     res;
	globus_off_t                        read_length;
	globus_off_t                        nbread;
	globus_off_t                        start_offset;
	globus_byte_t *                     buffer;
    unsigned long                       adler;
	checksum_block_list_t**                  checksum_array;
    checksum_block_list_t *             checksum_list_pp;
    unsigned long                       index;
    unsigned long                       i;
    unsigned long                       file_checksum;
    char                                ckSumbuf[CA_MAXCKSUMLEN+1];
    char                                ckSumbufdisk[CA_MAXCKSUMLEN+1];
    char                                ckSumnamedisk[CA_MAXCKSUMNAMELEN+1];
    char                                useCksum;
    int                                 xattr_len;
	char *		  		                func = "globus_l_gfs_CASTOR2int_send_next_to_client";

	GlobusGFSName(globus_l_gfs_CASTOR2int_send_next_to_client);
        
	if (CASTOR2int_handle->blk_length == 0) {
		/* check the next range to read */
		globus_gridftp_server_get_read_range(
				CASTOR2int_handle->op,
				&CASTOR2int_handle->blk_offset,
				&CASTOR2int_handle->blk_length);
		if(CASTOR2int_handle->blk_length == 0) {
			result = GLOBUS_SUCCESS;
			close(CASTOR2int_handle->fd);
	                CASTOR2int_handle->cached_res = result;
			CASTOR2int_handle->done = GLOBUS_TRUE;
			if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
			return CASTOR2int_handle->done;
		}
	}
	
	if (CASTOR2int_handle->blk_length == -1 ||
      (globus_size_t)CASTOR2int_handle->blk_length > CASTOR2int_handle->block_size)
    read_length = CASTOR2int_handle->block_size;
	else read_length = CASTOR2int_handle->blk_length;
	
	start_offset = lseek64(CASTOR2int_handle->fd, CASTOR2int_handle->blk_offset, SEEK_SET);
	/* verify that it worked */
	if(start_offset != CASTOR2int_handle->blk_offset) {
		result = globus_l_gfs_make_error("seek");
		close(CASTOR2int_handle->fd);
	        CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		return CASTOR2int_handle->done;
	}
	
	buffer = globus_malloc(read_length);
	if(buffer == NULL) {
		result = GlobusGFSErrorGeneric("error: malloc failed");
		close(CASTOR2int_handle->fd);
	    CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		return CASTOR2int_handle->done;
	}
	
	nbread = read(CASTOR2int_handle->fd, buffer, read_length);
    if(nbread>0) {
        if (CASTOR2int_handle->useCksum) {
            /* fill the checksum list  */
            adler = adler32(0L, Z_NULL, 0);
            adler = adler32(adler, buffer, nbread);
            
            CASTOR2int_handle->checksum_list_p->next=(checksum_block_list_t *)globus_malloc(sizeof(checksum_block_list_t));
            
            if (CASTOR2int_handle->checksum_list_p->next==NULL) {
                globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: malloc error \n",func);
                globus_free(buffer);
                close(CASTOR2int_handle->fd);
                CASTOR2int_handle->cached_res = GLOBUS_FAILURE;
                CASTOR2int_handle->done = GLOBUS_TRUE;
                if (CASTOR2int_handle->outstanding == 0)  globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
                globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: finished (error)\n",func);
                return CASTOR2int_handle->done;
            }
            CASTOR2int_handle->checksum_list_p->next->next=NULL;
            CASTOR2int_handle->checksum_list_p->offset=CASTOR2int_handle->blk_offset;
            CASTOR2int_handle->checksum_list_p->size=nbread;
            CASTOR2int_handle->checksum_list_p->csumvalue=adler;
            CASTOR2int_handle->checksum_list_p=CASTOR2int_handle->checksum_list_p->next;
            CASTOR2int_handle->number_of_blocks++;
            /* end of the checksum section */
        }
	}
	if(nbread == 0) { /* eof */
		result = GLOBUS_SUCCESS; 
		globus_free(buffer);
		
		if (CASTOR2int_handle->useCksum) {
            /* checksum calculation */
            checksum_array=(checksum_block_list_t**)globus_calloc(CASTOR2int_handle->number_of_blocks,sizeof(checksum_block_list_t*));
            if (checksum_array==NULL){
                free_checksum_list(CASTOR2int_handle->checksum_list);
                CASTOR2int_handle->cached_res = GLOBUS_FAILURE;
                globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: malloc error \n",func);
                CASTOR2int_handle->done = GLOBUS_TRUE;
                if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
                globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: finished (error)\n",func);
                close(CASTOR2int_handle->fd);
                return CASTOR2int_handle->done;
            }
            checksum_list_pp=CASTOR2int_handle->checksum_list;
            /* sorting of the list to the array */
            index = 0;
            while (checksum_list_pp->next != NULL) { /* the latest block is always empty and has next pointer as NULL */
              checksum_array[index] = checksum_list_pp;
              checksum_list_pp=checksum_list_pp->next;
              index++;
            }
            qsort(checksum_array, index, sizeof(checksum_block_list_t*), offsetComparison);
            /* combine here  */
            /* ************* */
            file_checksum=checksum_array[0]->csumvalue;
            for (i=1;i<CASTOR2int_handle->number_of_blocks;i++) {
              file_checksum=adler32_combine_(file_checksum,checksum_array[i]->csumvalue,checksum_array[i]->size);
            }
            globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: checksum for %s : AD 0x%lx\n",func,CASTOR2int_handle->fullDestPath,file_checksum);
            globus_free(checksum_array);
            free_checksum_list(CASTOR2int_handle->checksum_list);
            /* get extended attributes */
            useCksum=1;
            if ((xattr_len = fgetxattr(CASTOR2int_handle->fd,"user.castor.checksum.type",ckSumnamedisk,CA_MAXCKSUMNAMELEN)) == -1) {
                /* no error messages */
                useCksum = 0;
            } else {
                ckSumnamedisk[xattr_len] = '\0';
                if ((xattr_len = fgetxattr(CASTOR2int_handle->fd,"user.castor.checksum.value",ckSumbufdisk,CA_MAXCKSUMLEN)) == -1) {
                    /* no error messages */
                    useCksum = 0;
                } else {
                  ckSumbufdisk[xattr_len] = '\0';
                  if (strncmp(ckSumnamedisk,"ADLER32",CA_MAXCKSUMNAMELEN) != 0) useCksum=1; /* for gridftp we know only ADLER32 */
                }
            }
            
            if (useCksum) { /* we have disks and on the fly checksums here */
              sprintf(ckSumbuf,"%lx", file_checksum);
              if (strncmp(ckSumbufdisk,ckSumbuf,CA_MAXCKSUMLEN)==0) {
                globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: checksums OK! \n",func);
              } else {
                    globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: checksum error detected reading file: %s (recorded checksum: 0x%s calculated checksum: 0x%s)\n",func,
                    CASTOR2int_handle->fullDestPath,ckSumbufdisk, ckSumbuf);
                    /* to do something in error case */
                    CASTOR2int_handle->cached_res = globus_error_put (globus_object_construct (GLOBUS_ERROR_TYPE_BAD_DATA));
                    CASTOR2int_handle->done = GLOBUS_TRUE;
                    if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
                    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: finished (error)\n",func);
                    close(CASTOR2int_handle->fd);
                    return CASTOR2int_handle->done;
              }
            } else globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: ADLER32 checksum has not been found in extended attributes\n",func);
            /* end of checksum section */
		}
        close(CASTOR2int_handle->fd);
        
	    CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) { 
			globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		}
		globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: finished (eof)\n",func);
		return CASTOR2int_handle->done;
	}
	if(nbread < 0) { /* error */
		result = globus_l_gfs_make_error("read"); 
		globus_free(buffer);
		close(CASTOR2int_handle->fd);
		CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) { 
			globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		}
		globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: finished (error)\n",func);
		return CASTOR2int_handle->done;

	}
	
	if(read_length>=nbread) {
		/* if we have a file with size less than block_size we do not have use parrallel connections (one will be enough) */ 
		CASTOR2int_handle->optimal_count--;
	}
	read_length = nbread;
	
	if(CASTOR2int_handle->blk_length != -1) CASTOR2int_handle->blk_length -= read_length;
	
	/* start offset? */
	res = globus_gridftp_server_register_write(
			CASTOR2int_handle->op, buffer, read_length, CASTOR2int_handle->blk_offset, -1,
			globus_l_gfs_net_write_cb, CASTOR2int_handle);
	
	CASTOR2int_handle->blk_offset += read_length;
	
	if(res != GLOBUS_SUCCESS) {
		globus_free(buffer);
		close(CASTOR2int_handle->fd);
		CASTOR2int_handle->cached_res = res;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		return CASTOR2int_handle->done;
	}
	
	CASTOR2int_handle->outstanding++;
	return GLOBUS_FALSE;
}	


static
void
globus_l_gfs_net_write_cb(
		globus_gfs_operation_t              op,
		globus_result_t                     result,
		globus_byte_t *                     buffer,
		globus_size_t                       nbytes,
		void *                              user_arg)
{
	globus_l_gfs_CASTOR2int_handle_t *         CASTOR2int_handle;
	char * 					func="globus_l_gfs_net_write_cb";

  (void)nbytes;
	CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
	
	globus_free(buffer); 
	globus_mutex_lock(&CASTOR2int_handle->mutex);
	{
		CASTOR2int_handle->outstanding--;
		if(result != GLOBUS_SUCCESS) {
			CASTOR2int_handle->cached_res = result;
			CASTOR2int_handle->done = GLOBUS_TRUE;
		}
		if(!CASTOR2int_handle->done)  globus_l_gfs_CASTOR2int_send_next_to_client(CASTOR2int_handle);
		else if (CASTOR2int_handle->outstanding == 0) {
			close(CASTOR2int_handle->fd);
			globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: finished transfer\n",func);
			globus_gridftp_server_finished_transfer(op, CASTOR2int_handle->cached_res);
		}
	}
	globus_mutex_unlock(&CASTOR2int_handle->mutex);
}


static
int
globus_l_gfs_CASTOR2int_activate(void);

static
int
globus_l_gfs_CASTOR2int_deactivate(void);

/*
 *  no need to change this
 */
static globus_gfs_storage_iface_t       globus_l_gfs_CASTOR2int_dsi_iface = 
{
    GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | GLOBUS_GFS_DSI_DESCRIPTOR_SENDER,
    globus_l_gfs_CASTOR2int_start,
    globus_l_gfs_CASTOR2int_destroy,
    NULL, /* list */
    globus_l_gfs_CASTOR2int_send,
    globus_l_gfs_CASTOR2int_recv,
    NULL, /* trev */
    NULL, /* active */
    NULL, /* passive */
    NULL, /* data destroy */
    globus_l_gfs_CASTOR2int_command, 
    globus_l_gfs_CASTOR2int_stat,
    NULL, /* set_cred */
    NULL, /* buffer_send */
    NULL  /*realpath */
};


/*
 *  no need to change this
 */
GlobusExtensionDefineModule(globus_gridftp_server_CASTOR2int) =
{
    "globus_gridftp_server_CASTOR2int",
    globus_l_gfs_CASTOR2int_activate,
    globus_l_gfs_CASTOR2int_deactivate,
    NULL,
    NULL,
    &local_version,
    NULL
};

/*
 *  no need to change this
 */
static
int
globus_l_gfs_CASTOR2int_activate(void)
{
    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY,
        "CASTOR2int",
        GlobusExtensionMyModule(globus_gridftp_server_CASTOR2int),
        &globus_l_gfs_CASTOR2int_dsi_iface);
    
    return 0;
}

/*
 *  no need to change this
 */
static
int
globus_l_gfs_CASTOR2int_deactivate(void)
{
    globus_extension_registry_remove(
        GLOBUS_GFS_DSI_REGISTRY, "CASTOR2int");

    return 0;
}
