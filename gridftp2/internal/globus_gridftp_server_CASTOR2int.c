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
#define _LARGEFILE64_SOURCE
#define _LARGE_FILES
#endif

#include <sys/types.h>
#include <dirent.h>

#include "globus_gridftp_server.h"
#include "dsi_CASTOR2int.h"

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>


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

	err_str = globus_common_create_string("CASTOR2int Error: %s: %s", msg,  strerror(errno));
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
    
    globus_gridftp_server_operation_finished(
        op, GLOBUS_SUCCESS, &finished_info);
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
    globus_l_gfs_CASTOR2int_handle_t *     CASTOR2int_handle;
    
    char *                              func="globus_l_gfs_CASTOR2int_stat";
    struct stat64                       statbuf;
    int                                 status=0;
    globus_result_t                     result;
    char *                              pathname;
    
    GlobusGFSName(globus_l_gfs_CASTOR2int_stat);

    CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;

    pathname=strdup(stat_info->pathname);
    
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s\n",func,pathname);
    
    status=stat64(pathname,&statbuf);
    if(status!=0) {
	    result=globus_l_gfs_make_error("fstat64 failed");
	    globus_gridftp_server_finished_stat(op,result,NULL, 0);
	    free(pathname);
	    return;
    }
	
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: stat for the file: %s\n",func,pathname);
    stat_array = (globus_gfs_stat_t *) globus_calloc(1, sizeof(globus_gfs_stat_t));
    if(stat_array==NULL) {
       result=GlobusGFSErrorGeneric("CASTOR2int Error: memory allocation error");
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
    globus_l_gfs_CASTOR2int_handle_t *     CASTOR2int_handle;
    globus_result_t                     result;
    
    
    GlobusGFSName(globus_l_gfs_CASTOR2int_command);
    CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
    /* in gridftp disk server we do not allow to perform commads */
    result=GlobusGFSErrorGeneric("CASTOR2int Error: commands denied");
    globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
    return;
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
 
int CASTOR2int_handle_open(char *path, int flags, int mode, globus_l_gfs_CASTOR2int_handle_t * CASTOR2int_handle)
   {
   char *	host;
   int   	rc;
   char *	func="CASTOR2int_handle_open";
   char *       uuid_path;
   
   host=NULL;
   if(CASTOR2int_handle->use_uuid) { /* we will use fullDestPath instead of client "path", and "path" must be in uuid form */
       uuid_path=path;
       if( *uuid_path=='/') uuid_path++; /* path like  "/uuid" */
	  if(strcmp(uuid_path,CASTOR2int_handle->uuid)==0){
	      /* if clients uuid is the same as internal uuid we will access fullDestPath file then */
	      globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: open file in uuid mode \"%s\"\n",func,CASTOR2int_handle->fullDestPath);
	      rc = open64 (CASTOR2int_handle->fullDestPath,flags,mode);
	      return (rc);
	      }
	   globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: client and server uuids do not match in uuid mode \"%s\" != \"%s\"\n",
		      func, uuid_path,CASTOR2int_handle->uuid);
	   return (-1);
	   }
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
	char *				    func = "globus_l_gfs_file_net_read_cb";
	
	CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
        
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started \n",func);
	
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
				CASTOR2int_handle->cached_res = globus_l_gfs_make_error("seek failed");
				CASTOR2int_handle->done = GLOBUS_TRUE;
			}
			else {
				globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: write %ld byte to fd %d \n",func,nbytes,CASTOR2int_handle->fd);
				bytes_written = write(CASTOR2int_handle->fd, buffer, nbytes);
				if(bytes_written < nbytes) {
					CASTOR2int_handle->cached_res = globus_l_gfs_make_error("write failed");
					CASTOR2int_handle->done = GLOBUS_TRUE;
				}
			}
		}

		globus_free(buffer);
		/* if not done just register the next one */
		if(!CASTOR2int_handle->done) globus_l_gfs_CASTOR2int_read_from_net(CASTOR2int_handle);
		/* if done and there are no outstanding callbacks finish */
		else if(CASTOR2int_handle->outstanding == 0){
			close(CASTOR2int_handle->fd);
			globus_gridftp_server_finished_transfer(op, CASTOR2int_handle->cached_res);
		     }
	}
	globus_mutex_unlock(&CASTOR2int_handle->mutex);
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
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
	
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

	/* in the read case tis number will vary */
	globus_gridftp_server_get_optimal_concurrency(CASTOR2int_handle->op, &CASTOR2int_handle->optimal_count);
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: optimal concurrency: %u \n",func,CASTOR2int_handle->optimal_count);
	
	while(CASTOR2int_handle->outstanding < CASTOR2int_handle->optimal_count) {
		buffer=globus_malloc(CASTOR2int_handle->block_size);
		if (buffer == NULL) {
			result = GlobusGFSErrorGeneric("CASTOR2int Error: globus malloc failed");
			CASTOR2int_handle->cached_res = result;
			CASTOR2int_handle->done = GLOBUS_TRUE;
			if(CASTOR2int_handle->outstanding == 0) {
				close(CASTOR2int_handle->fd);
				globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
			}
			return;
		}
		globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: allocated %ld bytes \n",func,CASTOR2int_handle->block_size);
		result= globus_gridftp_server_register_read(
				CASTOR2int_handle->op,
				buffer,
				CASTOR2int_handle->block_size,
				globus_l_gfs_file_net_read_cb,
				CASTOR2int_handle);
		
		if(result != GLOBUS_SUCCESS)  {
			globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: register read has finished with a bad result \n",func);
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
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
}
   
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
	    result=GlobusGFSErrorGeneric("CASTOR2int Error: incorect access mode");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
       }
    pathname=strdup(transfer_info->pathname);
    if(pathname==NULL) {
	    result=GlobusGFSErrorGeneric("CASTOR2int Error: strdup failed");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s \n",func,pathname);
    
    /* try to open */
    flags = O_WRONLY | O_CREAT;
    if(transfer_info->truncate) flags |= O_TRUNC;
    
    CASTOR2int_handle->fd = CASTOR2int_handle_open(pathname, flags, 0644,CASTOR2int_handle);
    
    if(CASTOR2int_handle->fd <=0) {
	    result=globus_l_gfs_make_error("open/create error");
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
	    result=GlobusGFSErrorGeneric("CASTOR2int Error: incorect access mode");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
       }
    
    pathname=strdup(transfer_info->pathname);
    if (pathname == NULL) {
	result = GlobusGFSErrorGeneric("CASTOR2int Error: strdup failed");
	globus_gridftp_server_finished_transfer(op, result);
	return;
    }
    
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s\n",func,pathname);
    CASTOR2int_handle->fd = CASTOR2int_handle_open(pathname, O_RDONLY, 0644,CASTOR2int_handle);
    
    if(CASTOR2int_handle->fd <= 0) {
	result = globus_l_gfs_make_error("open failed");
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
	char *		  		    func = "globus_l_gfs_CASTOR2int_send_next_to_client";

	GlobusGFSName(globus_l_gfs_CASTOR2int_send_next_to_client);
        
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started \n",func);
	
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
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: offset:  %ld \n",func,CASTOR2int_handle->blk_offset);
	
	if(CASTOR2int_handle->blk_length == -1 ||	CASTOR2int_handle->blk_length > CASTOR2int_handle->block_size) read_length = CASTOR2int_handle->block_size;
	else read_length = CASTOR2int_handle->blk_length;
	
	start_offset = lseek64(CASTOR2int_handle->fd, CASTOR2int_handle->blk_offset, SEEK_SET);
	/* verify that it worked */
	if(start_offset != CASTOR2int_handle->blk_offset) {
		result = globus_l_gfs_make_error("seek failed");
		close(CASTOR2int_handle->fd);
	        CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		return CASTOR2int_handle->done;
	}
	
	buffer = globus_malloc(read_length);
	if(buffer == NULL) {
		result = GlobusGFSErrorGeneric("CASTOR2int Error: malloc failed");
		close(CASTOR2int_handle->fd);
	        CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		return CASTOR2int_handle->done;
	}
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: allocated %ld bytes \n",func,read_length);
	
	nbread = read(CASTOR2int_handle->fd, buffer, read_length);
	if(nbread <= 0) {
		result = GLOBUS_SUCCESS; /* this may just be eof */
		globus_free(buffer);
		close(CASTOR2int_handle->fd);
	        CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) { 
			globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
			globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished transfer\n",func);
		}
		globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished (eof)\n",func);
		return CASTOR2int_handle->done;
	}
	if(read_length>=nbread) {
		/* if we have a file with size less than block_size we do not have use parrallel connections (one will be enough) */ 
		CASTOR2int_handle->optimal_count--;
	}
	read_length = nbread;
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: read %ld bytes\n",func,read_length);
	
	if(CASTOR2int_handle->blk_length != -1) CASTOR2int_handle->blk_length -= read_length;
	
	/* start offset? */
	res = globus_gridftp_server_register_write(
			CASTOR2int_handle->op, buffer, read_length, CASTOR2int_handle->blk_offset, -1,
			globus_l_gfs_net_write_cb, CASTOR2int_handle);
	
	CASTOR2int_handle->blk_offset += read_length;
	
	if(res != GLOBUS_SUCCESS) {
		globus_free(buffer);
		close(CASTOR2int_handle->fd);
	        CASTOR2int_handle->cached_res = result;
		CASTOR2int_handle->done = GLOBUS_TRUE;
		if (CASTOR2int_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2int_handle->op, CASTOR2int_handle->cached_res);
		return CASTOR2int_handle->done;
	}
	
	CASTOR2int_handle->outstanding++;
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
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

	CASTOR2int_handle = (globus_l_gfs_CASTOR2int_handle_t *) user_arg;
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started \n",func);
	
	globus_free(buffer); 
	globus_mutex_lock(&CASTOR2int_handle->mutex);
	{
                globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: (outstanding: %u)\n",func,CASTOR2int_handle->outstanding);
		CASTOR2int_handle->outstanding--;
		if(result != GLOBUS_SUCCESS) {
			CASTOR2int_handle->cached_res = result;
			CASTOR2int_handle->done = GLOBUS_TRUE;
		}
		if(!CASTOR2int_handle->done)  globus_l_gfs_CASTOR2int_send_next_to_client(CASTOR2int_handle);
		else if (CASTOR2int_handle->outstanding == 0) {
			close(CASTOR2int_handle->fd);
			globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished transfer\n",func);
			globus_gridftp_server_finished_transfer(op, CASTOR2int_handle->cached_res);
		}
	}
	globus_mutex_unlock(&CASTOR2int_handle->mutex);
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);

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
    NULL,
    NULL
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
    &local_version
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
