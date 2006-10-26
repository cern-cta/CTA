/******************************************************************************
 *                  globus_gridftp_server_CASTOR2ext.c
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
#endif

#include <sys/types.h>
#include <dirent.h>

#include "globus_gridftp_server.h"
#include "dsi_CASTOR2ext.h"

#include <shift.h> 
#include <shift/Castor_limits.h>


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
globus_l_gfs_rfio_make_error(
		const char *                        msg)
{
	char *                              err_str;
	globus_result_t                     result;
	
	GlobusGFSName(globus_l_gfs_rfio_make_error);

	err_str = globus_common_create_string("CASTOR2 Error: %s: %s", msg,  rfio_serror());
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
globus_l_gfs_CASTOR2ext_start(
    globus_gfs_operation_t              op,
    globus_gfs_session_info_t *         session_info)
{
    globus_l_gfs_CASTOR2ext_handle_t *       CASTOR2ext_handle;
    globus_gfs_finished_info_t          finished_info;
    char *                              func="globus_l_gfs_CASTOR2ext_start";
    
    GlobusGFSName(globus_l_gfs_CASTOR2ext_start);

    CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *)
        globus_malloc(sizeof(globus_l_gfs_CASTOR2ext_handle_t));

        
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started, uid: %u, gid: %u\n",func, getuid(),getgid());
    globus_mutex_init(&CASTOR2ext_handle->mutex,NULL);
    
    memset(&finished_info, '\0', sizeof(globus_gfs_finished_info_t));
    finished_info.type = GLOBUS_GFS_OP_SESSION_START;
    finished_info.result = GLOBUS_SUCCESS;
    finished_info.info.session.session_arg = CASTOR2ext_handle;
    finished_info.info.session.username = session_info->username;
    finished_info.info.session.home_dir = NULL; /* if null we will go to HOME directory */

    CASTOR2ext_handle->use_uuid=GLOBUS_FALSE;
    CASTOR2ext_handle->uuid=NULL;
    CASTOR2ext_handle->fullDestPath=NULL;
    CASTOR2ext_handle->access_mode=NULL;
    if((CASTOR2ext_handle->uuid=getenv("UUID"))!=NULL) {
	    if((CASTOR2ext_handle->fullDestPath=getenv("FULLDESTPATH"))!=NULL) {
		    if((CASTOR2ext_handle->access_mode=getenv("ACCESS_MODE"))!=NULL) {
		       CASTOR2ext_handle->use_uuid=GLOBUS_TRUE;
		       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: uuid access is on: uuid=\"%s\" fullDestPath=\"%s\" mode=\"%s\"\n",
			                      func, CASTOR2ext_handle->uuid,CASTOR2ext_handle->fullDestPath,CASTOR2ext_handle->access_mode);
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
globus_l_gfs_CASTOR2ext_destroy(
    void *                              user_arg)
{
    globus_l_gfs_CASTOR2ext_handle_t *       CASTOR2ext_handle;

    CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *) user_arg;
    
    globus_mutex_destroy(&CASTOR2ext_handle->mutex);
    
    globus_free(CASTOR2ext_handle);
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
globus_l_gfs_CASTOR2ext_stat(
    globus_gfs_operation_t              op,
    globus_gfs_stat_info_t *            stat_info,
    void *                              user_arg)
{
    globus_gfs_stat_t *                 stat_array;
    int                                 stat_count;
    int					stat_count_tmp;
    globus_l_gfs_CASTOR2ext_handle_t *     CASTOR2ext_handle;
    
    char *                              func="globus_l_gfs_CASTOR2ext_stat";
    struct stat64                       statbuf;
    int                                 status=0;
    globus_result_t                     result;
    DIR *                               dp;
    int                                 nbsub;
    struct dirent64 *                   dirp;
    char *                              pathname;
    char                                filename[CA_MAXNAMELEN];
    
    GlobusGFSName(globus_l_gfs_CASTOR2ext_stat);

    CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *) user_arg;

    pathname=strdup(stat_info->pathname);
    
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s\n",func,pathname);
    
    status=rfio_stat64(pathname,&statbuf);
    if(status!=0) {
	    result=globus_l_gfs_rfio_make_error("rfio_stat64 failed");
	    globus_gridftp_server_finished_stat(op,result,NULL, 0);
	    free(pathname);
	    return;
    }
	
    if(stat_info->file_only) {
	    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: stat for the file: %s\n",func,pathname);
	    stat_array = (globus_gfs_stat_t *) globus_calloc(1, sizeof(globus_gfs_stat_t));
	    if(stat_array==NULL) {
		    result=GlobusGFSErrorGeneric("CASTOR2 Error: memory allocation error");
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
	
    if (S_ISDIR(statbuf.st_mode)) { /* directory */
	        globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: stats for the directory: %s\n",func,pathname);
		stat_count = 0;
		nbsub = 0;
		dp = rfio_opendir(pathname);
		if (dp==NULL) {
			result=globus_l_gfs_rfio_make_error("rfio_opendir failed");
	                globus_gridftp_server_finished_stat(op,result,NULL, 0);
	                free(pathname);
	                return;	
		}   
		while ((dirp = rfio_readdir64(dp)) != NULL) {
			stat_count++;
		}
		rfio_rewinddir(dp);
		if (stat_count == 0) { /* we do not have files in the directory */
			rfio_closedir(dp);
			stat_array = (globus_gfs_stat_t *) globus_calloc(1, sizeof(globus_gfs_stat_t));
	                if(stat_array==NULL) {
				result=GlobusGFSErrorGeneric("CASTOR2 Error: memory allocation error");
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
		/* we have files or subdirs */
		stat_count_tmp=0; /* if somebody will add a directory or a file we have memory only for the stat_count */
		stat_array = (globus_gfs_stat_t *)globus_calloc(stat_count, sizeof(globus_gfs_stat_t));
		while ((dirp = rfio_readdir64(dp)) != NULL && stat_count_tmp<stat_count) {
			stat_count_tmp++;
			memset(filename, 0, CA_MAXNAMELEN);
			sprintf(filename, "%s/%s", pathname, dirp->d_name);
			status = rfio_stat64(filename, &statbuf);
			if (status == 0) {
				fill_stat_array(&(stat_array[nbsub]), statbuf, dirp->d_name);
				nbsub++;
			}
			else {
				result=globus_l_gfs_rfio_make_error("rfio_stat64 inside readdir failed");
				globus_gridftp_server_finished_stat(op,result,NULL, 0);
				rfio_closedir(dp);
				free_stat_array(stat_array, stat_count);
				globus_free(stat_array);
				free(pathname);
				return;
			}
		}
		rfio_closedir(dp);
		globus_gridftp_server_finished_stat(op, GLOBUS_SUCCESS, stat_array, nbsub);
		free_stat_array(stat_array, stat_count);
		globus_free(stat_array);
		free(pathname);
		return;
    }
    /* it is not a file or directory? */
    stat_array = (globus_gfs_stat_t *) globus_calloc(1, sizeof(globus_gfs_stat_t));
    if(stat_array==NULL) {
	result=GlobusGFSErrorGeneric("CASTOR2 Error: memory allocation error");
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
globus_l_gfs_CASTOR2ext_command(
    globus_gfs_operation_t              op,
    globus_gfs_command_info_t *         cmd_info,
    void *                              user_arg)
{
    globus_l_gfs_CASTOR2ext_handle_t *     CASTOR2ext_handle;
    globus_result_t                     result;
    
    char *                              func="globus_l_gfs_CASTOR2ext_command";
    char *				pathname;
    char *				frm_pathname;
    int					status;
    
    GlobusGFSName(globus_l_gfs_CASTOR2ext_command);
    CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *) user_arg;

    if(CASTOR2ext_handle->use_uuid) { /* we use uuid mode to access files and do not allow to perform commands */
       result=GlobusGFSErrorGeneric("CASTOR2 Error: commands denied");
       globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
       return;
       }
       
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s\n",func,cmd_info->pathname);
    
    pathname=strdup(cmd_info->pathname);
    if(pathname == NULL) {
		result = GlobusGFSErrorGeneric("CASTOR2 Error: strdup failed");
		globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
		return;
	}
	
    if (*pathname != '/') {
		result = GlobusGFSErrorGeneric("CASTOR2 Error: bad file name");
		free(pathname);
		globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
		return;
	}
    /* TODO rfio setAuth here ? */  
    status=0;
    switch(cmd_info->command)
       {
       case GLOBUS_GFS_CMD_MKD:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_MKD: %s\n",func,pathname);
	       status = rfio_mkdir(pathname, 0755);
	       break;

       case GLOBUS_GFS_CMD_RMD:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_RMD: %s\n",func,pathname);
	       status = rfio_rmdir(pathname);
	       break;

       case GLOBUS_GFS_CMD_DELE:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_DELE: %s\n",func,pathname);
	       status = rfio_unlink(pathname);
	       break;

       case GLOBUS_GFS_CMD_RNTO:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_RNTO: %s to %s\n",func,pathname,cmd_info->rnfr_pathname);
	       frm_pathname = strdup(cmd_info->rnfr_pathname);
	       if(frm_pathname == NULL) {
		       result = GlobusGFSErrorGeneric("CASTOR2 Error: strdup failed");
		       globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
		       return;
	       }
	       if (*frm_pathname != '/'){
		       free(frm_pathname);
		       result = GlobusGFSErrorGeneric("CASTOR2 Error: bad pathname name");
		       free(pathname);
		       globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
		       return;
	       }
	       status = rfio_rename(frm_pathname, pathname);
	       free(frm_pathname);
	       break;
       case GLOBUS_GFS_CMD_RNFR:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_RNFR \n",func);
	       break;	       
       case GLOBUS_GFS_CMD_SITE_DSI:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_SITE_DSI \n",func);
	       break;	       
       case GLOBUS_GFS_CMD_CKSM:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_CKSM \n",func);
	       break;
       case GLOBUS_GFS_CMD_SITE_CHMOD:
	       globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: GLOBUS_GFS_CMD_SITE_CHMOD: %s (%o)\n",func,pathname,cmd_info->chmod_mode);
	       status = rfio_chmod(pathname, cmd_info->chmod_mode);
	       break;
       default:
	       break;
       }
	if(status!=0) {
		result=globus_l_gfs_rfio_make_error("rfio_api error");
		globus_gridftp_server_finished_command(op, result, GLOBUS_NULL);
		free(pathname);
		return;
	}
		
    globus_gridftp_server_finished_command(op, GLOBUS_SUCCESS, GLOBUS_NULL);
    free(pathname);
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
 
int CASTOR2ext_handle_open(char *path, int flags, int mode, globus_l_gfs_CASTOR2ext_handle_t * CASTOR2ext_handle)
   {
   char *	filename;
   char *	host;
   int   	rc;
   char *	func="CASTOR2ext_handle_open";
   char *       uuid_path;
   
   host=NULL;
   if ((rc = rfio_parse (path, &host, &filename)) < 0) return (-1);
   globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: path has been parsed: %s:%s (%u) \n",func,host,filename,rc);
   
   if(rc==0 && host!=NULL) { /* remote file  */
	   rc =rfio_open64 (path,flags,mode);
	   return (rc);
   }
   if(rc==0 && host==NULL) { /* local file  */
	   if(CASTOR2ext_handle->use_uuid) { /* we will use fullDestPath instead of client "path", and "path" must be in uuid form */
              uuid_path=path;
	      if( *uuid_path=='/') uuid_path++; /* path like  "/uuid" */
	      if(strcmp(uuid_path,CASTOR2ext_handle->uuid)==0){
		      /* if clients uuid is the same as internal uuid we will access fullDestPath file then */
		      globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: open file in uuid mode \"%s\"\n",func,CASTOR2ext_handle->fullDestPath);
		      rc = rfio_open64 (CASTOR2ext_handle->fullDestPath,flags,mode);
		      return (rc);
	      }
	      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: client and server uuids do not match in uuid mode \"%s\" != \"%s\"\n",
		      func, uuid_path,CASTOR2ext_handle->uuid);
	      return (-1);
	   }
	   rc =rfio_open64 (path,flags,mode);
	   return (rc);
   }
   return (-1);
   }

  /* receive from client */
static
void
globus_l_gfs_rfio_net_read_cb(
		globus_gfs_operation_t              op,
		globus_result_t                     result,
		globus_byte_t *                     buffer,
		globus_size_t                       nbytes,
		globus_off_t                        offset,
		globus_bool_t                       eof,
		void *                              user_arg)
{
	globus_off_t                        start_offset;
	globus_l_gfs_CASTOR2ext_handle_t *     CASTOR2ext_handle;
	globus_size_t                       bytes_written;
	char *				    func = "globus_l_gfs_rfio_net_read_cb";
	
	CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *) user_arg;
        
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started \n",func);
	
	globus_mutex_lock(&CASTOR2ext_handle->mutex);
	{
		if(eof)	CASTOR2ext_handle->done = GLOBUS_TRUE;
		CASTOR2ext_handle->outstanding--;
		if(result != GLOBUS_SUCCESS) {
			CASTOR2ext_handle->cached_res = result;
			CASTOR2ext_handle->done = GLOBUS_TRUE;
		}
		else if(nbytes > 0) {
			start_offset = rfio_lseek64(CASTOR2ext_handle->fd, offset, SEEK_SET);
			if(start_offset != offset) {
				CASTOR2ext_handle->cached_res = globus_l_gfs_rfio_make_error("seek failed");
				CASTOR2ext_handle->done = GLOBUS_TRUE;
			}
			else {
				globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: write %ld byte to fd %d \n",func,nbytes,CASTOR2ext_handle->fd);
				bytes_written = rfio_write(CASTOR2ext_handle->fd, buffer, nbytes);
				if(bytes_written < nbytes) {
					CASTOR2ext_handle->cached_res = globus_l_gfs_rfio_make_error("write failed");
					CASTOR2ext_handle->done = GLOBUS_TRUE;
				}
			}
		}

		globus_free(buffer);
		/* if not done just register the next one */
		if(!CASTOR2ext_handle->done) globus_l_gfs_CASTOR2ext_read_from_net(CASTOR2ext_handle);
		/* if done and there are no outstanding callbacks finish */
		else if(CASTOR2ext_handle->outstanding == 0){
			rfio_close(CASTOR2ext_handle->fd);
			globus_gridftp_server_finished_transfer(op, CASTOR2ext_handle->cached_res);
		     }
	}
	globus_mutex_unlock(&CASTOR2ext_handle->mutex);
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
}
 
   
static
void
globus_l_gfs_CASTOR2ext_read_from_net(
		globus_l_gfs_CASTOR2ext_handle_t *         CASTOR2ext_handle)
{
	globus_byte_t *                     buffer;
	globus_result_t                     result;
	char * 				    func="globus_l_gfs_CASTOR2ext_read_from_net";
	
	GlobusGFSName(globus_l_gfs_CASTOR2ext_read_from_net);
	
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

	/* in the read case tis number will vary */
	globus_gridftp_server_get_optimal_concurrency(CASTOR2ext_handle->op, &CASTOR2ext_handle->optimal_count);
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: optimal concurrency: %u \n",func,CASTOR2ext_handle->optimal_count);
	
	while(CASTOR2ext_handle->outstanding < CASTOR2ext_handle->optimal_count) {
		buffer=globus_malloc(CASTOR2ext_handle->block_size);
		if (buffer == NULL) {
			result = GlobusGFSErrorGeneric("CASTOR2 Error: globus malloc failed");
			CASTOR2ext_handle->cached_res = result;
			CASTOR2ext_handle->done = GLOBUS_TRUE;
			if(CASTOR2ext_handle->outstanding == 0) {
				rfio_close(CASTOR2ext_handle->fd);
				globus_gridftp_server_finished_transfer(CASTOR2ext_handle->op, CASTOR2ext_handle->cached_res);
			}
			return;
		}
		globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: allocated %ld bytes \n",func,CASTOR2ext_handle->block_size);
		result= globus_gridftp_server_register_read(
				CASTOR2ext_handle->op,
				buffer,
				CASTOR2ext_handle->block_size,
				globus_l_gfs_rfio_net_read_cb,
				CASTOR2ext_handle);
		
		if(result != GLOBUS_SUCCESS)  {
			globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: register read has finished with a bad result \n",func);
			globus_free(buffer);
			CASTOR2ext_handle->cached_res = result;
			CASTOR2ext_handle->done = GLOBUS_TRUE;
			if(CASTOR2ext_handle->outstanding == 0) {
				rfio_close(CASTOR2ext_handle->fd);
				globus_gridftp_server_finished_transfer(CASTOR2ext_handle->op, CASTOR2ext_handle->cached_res);
			}
			return;
		}
		CASTOR2ext_handle->outstanding++;
	}
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
}
   
static
void
globus_l_gfs_CASTOR2ext_recv(
    globus_gfs_operation_t              op,
    globus_gfs_transfer_info_t *        transfer_info,
    void *                              user_arg)
{
    globus_l_gfs_CASTOR2ext_handle_t *     CASTOR2ext_handle;
    
    globus_result_t                     result;
    char * 				func="globus_l_gfs_CASTOR2ext_recv";
    char * 				pathname;
    int 				flags;
    
    GlobusGFSName(globus_l_gfs_CASTOR2ext_recv);
    CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *) user_arg;
   
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);
    
    if(CASTOR2ext_handle->use_uuid) /* we use uuid mode to access files */
       if( strcmp(CASTOR2ext_handle->access_mode,"w") != 0 && strcmp(CASTOR2ext_handle->access_mode,"o") !=0 ) {
	    result=GlobusGFSErrorGeneric("CASTOR2 Error: incorect access mode");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
       }
    pathname=strdup(transfer_info->pathname);
    if(pathname==NULL) {
	    result=GlobusGFSErrorGeneric("CASTOR2 Error: strdup failed");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s \n",func,pathname);
    
    /* try to open */
    flags = O_WRONLY | O_CREAT;
    if(transfer_info->truncate) flags |= O_TRUNC;
    
    CASTOR2ext_handle->fd = CASTOR2ext_handle_open(pathname, flags, 0644,CASTOR2ext_handle);
    
    if(CASTOR2ext_handle->fd <=0) {
	    result=globus_l_gfs_rfio_make_error("rfio open/create error");
	    free(pathname);
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
    }
	    
   /* reset all the needed variables in the handle */
   CASTOR2ext_handle->cached_res = GLOBUS_SUCCESS;
   CASTOR2ext_handle->outstanding = 0;
   CASTOR2ext_handle->done = GLOBUS_FALSE;
   CASTOR2ext_handle->blk_length = 0;
   CASTOR2ext_handle->blk_offset = 0;
   CASTOR2ext_handle->op = op;
   
   globus_gridftp_server_get_block_size(op, &CASTOR2ext_handle->block_size);
   globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: block size: %ld\n",func,CASTOR2ext_handle->block_size);
   
   globus_gridftp_server_begin_transfer(op, 0, CASTOR2ext_handle);
   
   globus_mutex_lock(&CASTOR2ext_handle->mutex);
      {
      globus_l_gfs_CASTOR2ext_read_from_net(CASTOR2ext_handle);
      }
   globus_mutex_unlock(&CASTOR2ext_handle->mutex);
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
globus_l_gfs_CASTOR2ext_send(
    globus_gfs_operation_t              op,
    globus_gfs_transfer_info_t *        transfer_info,
    void *                              user_arg)
{
    globus_l_gfs_CASTOR2ext_handle_t *       CASTOR2ext_handle;
    char * 				func="globus_l_gfs_CASTOR2ext_send";
    char *				pathname;
    int 				i;
    globus_bool_t                       done;
    globus_result_t                     result;
    
    GlobusGFSName(globus_l_gfs_CASTOR2ext_send);
    CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *) user_arg;
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);
    
    if(CASTOR2ext_handle->use_uuid) /* we use uuid mode to access files */
       if( strcmp(CASTOR2ext_handle->access_mode,"r") != 0 && strcmp(CASTOR2ext_handle->access_mode,"o") !=0 ) {
	    result=GlobusGFSErrorGeneric("CASTOR2 Error: incorect access mode");
	    globus_gridftp_server_finished_transfer(op, result);
	    return;
       }
    
    pathname=strdup(transfer_info->pathname);
    if (pathname == NULL) {
	result = GlobusGFSErrorGeneric("CASTOR2 Error: strdup failed");
	globus_gridftp_server_finished_transfer(op, result);
	return;
    }
    
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: pathname: %s\n",func,pathname);
    CASTOR2ext_handle->fd = CASTOR2ext_handle_open(pathname, O_RDONLY, 0644,CASTOR2ext_handle);
    
    if(CASTOR2ext_handle->fd <= 0) {
	result = globus_l_gfs_rfio_make_error("open failed");
	free(pathname);
	globus_gridftp_server_finished_transfer(op, result);
	return;
    }
    
    /* reset all the needed variables in the handle */
    CASTOR2ext_handle->cached_res = GLOBUS_SUCCESS;
    CASTOR2ext_handle->outstanding = 0;
    CASTOR2ext_handle->done = GLOBUS_FALSE;
    CASTOR2ext_handle->blk_length = 0;
    CASTOR2ext_handle->blk_offset = 0;
    CASTOR2ext_handle->op = op;
    
    globus_gridftp_server_get_optimal_concurrency(op, &CASTOR2ext_handle->optimal_count);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: optimal_concurrency: %u\n",func,CASTOR2ext_handle->optimal_count);
    
    globus_gridftp_server_get_block_size(op, &CASTOR2ext_handle->block_size);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: block_size: %ld\n",func,CASTOR2ext_handle->block_size);
    
    globus_gridftp_server_begin_transfer(op, 0, CASTOR2ext_handle);
    done = GLOBUS_FALSE;
    globus_mutex_lock(&CASTOR2ext_handle->mutex);
    {
	   for(i = 0; i < CASTOR2ext_handle->optimal_count && !done; i++) {
		done = globus_l_gfs_CASTOR2ext_send_next_to_client(CASTOR2ext_handle);
	   }
    }
    globus_mutex_unlock(&CASTOR2ext_handle->mutex);
    free(pathname);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
}

static
globus_bool_t
globus_l_gfs_CASTOR2ext_send_next_to_client(
		globus_l_gfs_CASTOR2ext_handle_t *         CASTOR2ext_handle)
{
	globus_result_t                     result;
	globus_result_t                     res;
	globus_off_t                        read_length;
	globus_off_t                        nbread;
	globus_off_t                        start_offset;
	globus_byte_t *                     buffer;
	char *		  		    func = "globus_l_gfs_CASTOR2ext_send_next_to_client";

	GlobusGFSName(globus_l_gfs_CASTOR2ext_send_next_to_client);
        
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started \n",func);
	
	if (CASTOR2ext_handle->blk_length == 0) {
		/* check the next range to read */
		globus_gridftp_server_get_read_range(
				CASTOR2ext_handle->op,
				&CASTOR2ext_handle->blk_offset,
				&CASTOR2ext_handle->blk_length);
		if(CASTOR2ext_handle->blk_length == 0) {
			result = GLOBUS_SUCCESS;
			rfio_close(CASTOR2ext_handle->fd);
	                CASTOR2ext_handle->cached_res = result;
			CASTOR2ext_handle->done = GLOBUS_TRUE;
			if (CASTOR2ext_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2ext_handle->op, CASTOR2ext_handle->cached_res);
			return CASTOR2ext_handle->done;
		}
	}
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: offset:  %ld \n",func,CASTOR2ext_handle->blk_offset);
	
	if(CASTOR2ext_handle->blk_length == -1 ||	CASTOR2ext_handle->blk_length > CASTOR2ext_handle->block_size) read_length = CASTOR2ext_handle->block_size;
	else read_length = CASTOR2ext_handle->blk_length;
	
	start_offset = rfio_lseek64(CASTOR2ext_handle->fd, CASTOR2ext_handle->blk_offset, SEEK_SET);
	/* verify that it worked */
	if(start_offset != CASTOR2ext_handle->blk_offset) {
		result = globus_l_gfs_rfio_make_error("seek failed");
		rfio_close(CASTOR2ext_handle->fd);
	        CASTOR2ext_handle->cached_res = result;
		CASTOR2ext_handle->done = GLOBUS_TRUE;
		if (CASTOR2ext_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2ext_handle->op, CASTOR2ext_handle->cached_res);
		return CASTOR2ext_handle->done;
	}
	
	buffer = globus_malloc(read_length);
	if(buffer == NULL) {
		result = GlobusGFSErrorGeneric("CASTOR2 Error: malloc failed");
		rfio_close(CASTOR2ext_handle->fd);
	        CASTOR2ext_handle->cached_res = result;
		CASTOR2ext_handle->done = GLOBUS_TRUE;
		if (CASTOR2ext_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2ext_handle->op, CASTOR2ext_handle->cached_res);
		return CASTOR2ext_handle->done;
	}
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: allocated %ld bytes \n",func,read_length);
	
	nbread = rfio_read(CASTOR2ext_handle->fd, buffer, read_length);
	if(nbread <= 0) {
		result = GLOBUS_SUCCESS; /* this may just be eof */
		globus_free(buffer);
		rfio_close(CASTOR2ext_handle->fd);
	        CASTOR2ext_handle->cached_res = result;
		CASTOR2ext_handle->done = GLOBUS_TRUE;
		if (CASTOR2ext_handle->outstanding == 0) { 
			globus_gridftp_server_finished_transfer(CASTOR2ext_handle->op, CASTOR2ext_handle->cached_res);
			globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished transfer\n",func);
		}
		globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished (eof)\n",func);
		return CASTOR2ext_handle->done;
	}
	if(read_length>=nbread) {
		/* if we have a file with size less than block_size we do not have use parrallel connections (one will be enough) */ 
		CASTOR2ext_handle->optimal_count--;
	}
	read_length = nbread;
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: read %ld bytes\n",func,read_length);
	
	if(CASTOR2ext_handle->blk_length != -1) CASTOR2ext_handle->blk_length -= read_length;
	
	/* start offset? */
	res = globus_gridftp_server_register_write(
			CASTOR2ext_handle->op, buffer, read_length, CASTOR2ext_handle->blk_offset, -1,
			globus_l_gfs_net_write_cb, CASTOR2ext_handle);
	
	CASTOR2ext_handle->blk_offset += read_length;
	
	if(res != GLOBUS_SUCCESS) {
		globus_free(buffer);
		rfio_close(CASTOR2ext_handle->fd);
	        CASTOR2ext_handle->cached_res = result;
		CASTOR2ext_handle->done = GLOBUS_TRUE;
		if (CASTOR2ext_handle->outstanding == 0) globus_gridftp_server_finished_transfer(CASTOR2ext_handle->op, CASTOR2ext_handle->cached_res);
		return CASTOR2ext_handle->done;
	}
	
	CASTOR2ext_handle->outstanding++;
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
	globus_l_gfs_CASTOR2ext_handle_t *         CASTOR2ext_handle;
	char * 					func="globus_l_gfs_net_write_cb";

	CASTOR2ext_handle = (globus_l_gfs_CASTOR2ext_handle_t *) user_arg;
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started \n",func);
	
	globus_free(buffer); 
	globus_mutex_lock(&CASTOR2ext_handle->mutex);
	{
                globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: (outstanding: %u)\n",func,CASTOR2ext_handle->outstanding);
		CASTOR2ext_handle->outstanding--;
		if(result != GLOBUS_SUCCESS) {
			CASTOR2ext_handle->cached_res = result;
			CASTOR2ext_handle->done = GLOBUS_TRUE;
		}
		if(!CASTOR2ext_handle->done)  globus_l_gfs_CASTOR2ext_send_next_to_client(CASTOR2ext_handle);
		else if (CASTOR2ext_handle->outstanding == 0) {
			rfio_close(CASTOR2ext_handle->fd);
			globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished transfer\n",func);
			globus_gridftp_server_finished_transfer(op, CASTOR2ext_handle->cached_res);
		}
	}
	globus_mutex_unlock(&CASTOR2ext_handle->mutex);
	globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);

}


static
int
globus_l_gfs_CASTOR2ext_activate(void);

static
int
globus_l_gfs_CASTOR2ext_deactivate(void);

/*
 *  no need to change this
 */
static globus_gfs_storage_iface_t       globus_l_gfs_CASTOR2ext_dsi_iface = 
{
    GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | GLOBUS_GFS_DSI_DESCRIPTOR_SENDER,
    globus_l_gfs_CASTOR2ext_start,
    globus_l_gfs_CASTOR2ext_destroy,
    NULL, /* list */
    globus_l_gfs_CASTOR2ext_send,
    globus_l_gfs_CASTOR2ext_recv,
    NULL, /* trev */
    NULL, /* active */
    NULL, /* passive */
    NULL, /* data destroy */
    globus_l_gfs_CASTOR2ext_command, 
    globus_l_gfs_CASTOR2ext_stat,
    NULL,
    NULL
};


/*
 *  no need to change this
 */
GlobusExtensionDefineModule(globus_gridftp_server_CASTOR2) =
{
    "globus_gridftp_server_CASTOR2ext",
    globus_l_gfs_CASTOR2ext_activate,
    globus_l_gfs_CASTOR2ext_deactivate,
    NULL,
    NULL,
    &local_version
};

/*
 *  no need to change this
 */
static
int
globus_l_gfs_CASTOR2ext_activate(void)
{
    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY,
        "CASTOR2ext",
        GlobusExtensionMyModule(globus_gridftp_server_CASTOR2),
        &globus_l_gfs_CASTOR2ext_dsi_iface);
    
    return 0;
}

/*
 *  no need to change this
 */
static
int
globus_l_gfs_CASTOR2ext_deactivate(void)
{
    globus_extension_registry_remove(
        GLOBUS_GFS_DSI_REGISTRY, "CASTOR2ext");

    return 0;
}
