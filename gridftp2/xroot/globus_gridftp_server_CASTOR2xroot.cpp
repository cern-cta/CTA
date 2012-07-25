/******************************************************************************
 *                  globus_gridftp_server_CASTOR2xroot.c
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
 * @(#)$RCSfile: globus_gridftp_server_CASTOR2xroot.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2009/05/13 09:59:57 $ $Author: sponcec3 $
 *
 * Plugin for GridFTP interfacing to CASTOR using the xroot protocol
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#if defined(linux)
#define _LARGE_FILES
#endif

#include <sys/types.h>
#include <dirent.h>
#include <string.h>

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdPosix/XrdPosixExtern.hh>
#include "dsi_CASTOR2xroot.hpp"

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

extern "C" {

  static globus_version_t local_version =
    {
      0, /* major version number */
      1, /* minor version number */
      1157544130,
      0 /* branch ID */
    };

  /// utility function to make errors
  static globus_result_t
  globus_l_gfs_make_error(const char *msg, int errCode) {
    char *err_str;
    globus_result_t result;
    GlobusGFSName(globus_l_gfs_make_error);
    err_str = globus_common_create_string
      ("%s error: %s", msg,  strerror(errCode));
    result = GlobusGFSErrorGeneric(err_str);
    globus_free(err_str);
    return result;
  }

  /* fill the statbuf into globus_gfs_stat_t */
  void fill_stat_array(globus_gfs_stat_t * filestat,
                       struct stat statbuf,
                       char *name) {
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
  void free_stat_array(globus_gfs_stat_t * filestat, int count) {
    int i;
    for(i=0;i<count;i++) free(filestat[i].name);
  }

  /*************************************************************************
   *  start
   *  -----
   *  This function is called when a new session is initialized, ie a user
   *  connects to the server.  This hook gives the dsi an opportunity to
   *  set internal state that will be threaded through to all other
   *  function calls associated with this session.  And an opportunity to
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
  globus_l_gfs_CASTOR2xroot_start(globus_gfs_operation_t op,
                                  globus_gfs_session_info_t *session_info) {
    globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle;
    globus_gfs_finished_info_t finished_info;
    const char *func="globus_l_gfs_CASTOR2xroot_start";

    GlobusGFSName(globus_l_gfs_CASTOR2xroot_start);

    CASTOR2xroot_handle = (globus_l_gfs_CASTOR2xroot_handle_t *)
      globus_malloc(sizeof(globus_l_gfs_CASTOR2xroot_handle_t));

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: started, uid: %u, gid: %u\n",
                           func, getuid(),getgid());
    globus_mutex_init(&CASTOR2xroot_handle->mutex,NULL);

    memset(&finished_info, '\0', sizeof(globus_gfs_finished_info_t));
    finished_info.type = GLOBUS_GFS_OP_SESSION_START;
    finished_info.result = GLOBUS_SUCCESS;
    finished_info.info.session.session_arg = CASTOR2xroot_handle;
    finished_info.info.session.username = session_info->username;
    // if null we will go to HOME directory
    finished_info.info.session.home_dir = NULL;

    CASTOR2xroot_handle->use_uuid=GLOBUS_FALSE;
    CASTOR2xroot_handle->uuid=NULL;
    CASTOR2xroot_handle->fullDestPath=NULL;
    CASTOR2xroot_handle->access_mode=NULL;
    if((CASTOR2xroot_handle->uuid=getenv("UUID"))!=NULL) {
      if((CASTOR2xroot_handle->fullDestPath=getenv("FULLDESTPATH"))!=NULL) {
        if((CASTOR2xroot_handle->access_mode=getenv("ACCESS_MODE"))!=NULL) {
          CASTOR2xroot_handle->use_uuid=GLOBUS_TRUE;
          globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                                 "%s: uuid access is on: uuid=\"%s\" "
                                 "fullDestPath=\"%s\" mode=\"%s\"\n",
                                 func, CASTOR2xroot_handle->uuid,
                                 CASTOR2xroot_handle->fullDestPath,
                                 CASTOR2xroot_handle->access_mode);
        }
      }
    }
    globus_gridftp_server_operation_finished(op, GLOBUS_SUCCESS, &finished_info);
  }

  /*************************************************************************
   *  destroy
   *  -------
   *  This is called when a session ends, ie client quits or disconnects.
   *  The dsi should clean up all memory they associated wit the session
   *  here.
   ************************************************************************/
  static void
  globus_l_gfs_CASTOR2xroot_destroy(void *user_arg) {
    globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle;
    CASTOR2xroot_handle = (globus_l_gfs_CASTOR2xroot_handle_t *) user_arg;
    globus_mutex_destroy(&CASTOR2xroot_handle->mutex);
    globus_free(CASTOR2xroot_handle);
  }

  /*************************************************************************
   *  stat
   *  ----
   *  This interface function is called whenever the server needs
   *  information about a given file or resource.  It is called then an
   *  LIST is sent by the client, when the server needs to verify that
   *  a file exists and has the proper permissions, etc.
   ************************************************************************/
  static void globus_l_gfs_CASTOR2xroot_stat(globus_gfs_operation_t op,
                                             globus_gfs_stat_info_t *stat_info,
                                             void *user_arg) {
    globus_gfs_stat_t *stat_array;
    int stat_count;
    globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle;

    const char *func = "globus_l_gfs_CASTOR2xroot_stat";
    struct stat statbuf;
    int status = 0;
    globus_result_t result;
    char *pathname;
    char *uuid_path;

    GlobusGFSName(globus_l_gfs_CASTOR2xroot_stat);
    CASTOR2xroot_handle = (globus_l_gfs_CASTOR2xroot_handle_t *) user_arg;

    // we will use fullDestPath instead of client "path",
    // and "path" must be in uuid form
    int uuid_ok = 0;
    if(CASTOR2xroot_handle->use_uuid) {
      uuid_path=stat_info->pathname;
      /* strip any preceding path to isolate only the uuid part; see also CASTOR2int_handle_open */
      while (*uuid_path=='/') uuid_path++;
      if(strcmp(uuid_path,CASTOR2xroot_handle->uuid)==0) {
        pathname = strdup(CASTOR2xroot_handle->fullDestPath);
        uuid_ok = 1;
      }
    }
    // uuid check has failed
    if (0 == uuid_ok) {
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                             "%s: client and server uuids do not match "
                             "in uuid mode \"%s\" != \"%s\"\n",
                             func, uuid_path,
                             CASTOR2xroot_handle->uuid);
      result=GlobusGFSErrorGeneric("uuid mismatch");
      globus_gridftp_server_finished_stat(op,result,NULL, 0);
      return;
    }

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: pathname: %s\n",
                           func,pathname);
    status=XrdPosix_Stat(pathname,&statbuf);
    if(status!=0) {
      result=globus_l_gfs_make_error("XrdPosix_Stat", errno);
      globus_gridftp_server_finished_stat(op,result,NULL, 0);
      free(pathname);
      return;
    }

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: stat for the file: %s\n",
                           func,pathname);
    stat_array = (globus_gfs_stat_t *)
      globus_calloc(1, sizeof(globus_gfs_stat_t));
    if(stat_array==NULL) {
      result=GlobusGFSErrorGeneric("error: memory allocation failed");
      globus_gridftp_server_finished_stat(op,result,NULL, 0);
      free(pathname);
      return;
    }
    stat_count=1;
    fill_stat_array(&(stat_array[0]),statbuf,pathname);
    globus_gridftp_server_finished_stat(op, GLOBUS_SUCCESS,
                                        stat_array, stat_count);
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
  static void
  globus_l_gfs_CASTOR2xroot_command(globus_gfs_operation_t op,
                                    globus_gfs_command_info_t*,
                                    void*) {
    globus_result_t result;

    GlobusGFSName(globus_l_gfs_CASTOR2xroot_command);
    /* in gridftp disk server we do not allow to perform commads */
    result=GlobusGFSErrorGeneric("error: commands denied");
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

  int CASTOR2xroot_handle_open
  (char *path, int flags, int mode,
   globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle) {
    int rc;
    const char *func = "CASTOR2xroot_handle_open";
    char *uuid_path;
    // we will use fullDestPath instead of client "path",
    // and "path" must be in uuid form
    if(CASTOR2xroot_handle->use_uuid) {
      uuid_path=path;
      while(strchr(uuid_path, '/') != NULL) {
        // strip any preceding path to isolate only the uuid part
        uuid_path = strchr(uuid_path, '/') + 1;
      }
      if(strcmp(uuid_path,CASTOR2xroot_handle->uuid) == 0) {
        // if clients uuid is the same as internal uuid
        // we will access fullDestPath file then
        globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                               "%s: open file in uuid mode \"%s\"\n",
                               func,
                               CASTOR2xroot_handle->fullDestPath);
        try {
          rc = XrdPosixXrootd::Open(CASTOR2xroot_handle->fullDestPath, flags, mode);
          if (rc < 0) {
            globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,
                                   "%s: XrdPosixXrootd::Open returned error code %d\n",
                                   func, errno);
          }
        } catch (...) {
          globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                               "%s: Exception caught when calling XrdPosixXrootd::Open\n",
                               func);
          return -1;
        }
        return (rc);
      }
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                             "%s: client and server uuids do not match "
                             "in uuid mode \"%s\" != \"%s\"\n",
                             func, uuid_path,
                             CASTOR2xroot_handle->uuid);
      errno = ENOENT;
      return (-1);
    }
    errno = EINVAL;
    return (-1);
  }

  /* receive from client */
  static void globus_l_gfs_file_net_read_cb(globus_gfs_operation_t op,
                                            globus_result_t result,
                                            globus_byte_t *buffer,
                                            globus_size_t nbytes,
                                            globus_off_t offset,
                                            globus_bool_t eof,
                                            void *user_arg) {
    globus_off_t start_offset;
    globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle;
    globus_size_t bytes_written;

    CASTOR2xroot_handle = (globus_l_gfs_CASTOR2xroot_handle_t *) user_arg;
    globus_mutex_lock(&CASTOR2xroot_handle->mutex);
    {
      if(eof) CASTOR2xroot_handle->done = GLOBUS_TRUE;
      CASTOR2xroot_handle->outstanding--;
      if(result != GLOBUS_SUCCESS) {
        CASTOR2xroot_handle->cached_res = result;
        CASTOR2xroot_handle->done = GLOBUS_TRUE;
      }
      else if(nbytes > 0) {
        start_offset = XrdPosix_Lseek(CASTOR2xroot_handle->fd, offset, SEEK_SET);
        if(start_offset != offset) {
          CASTOR2xroot_handle->cached_res = globus_l_gfs_make_error("seek",errno);
          CASTOR2xroot_handle->done = GLOBUS_TRUE;
        }
        else {
          bytes_written = XrdPosix_Write(CASTOR2xroot_handle->fd, buffer, nbytes);
          if(bytes_written < nbytes) {
            errno = ENOSPC;
            CASTOR2xroot_handle->cached_res =
              globus_l_gfs_make_error("write",errno);
            CASTOR2xroot_handle->done = GLOBUS_TRUE;
          } else globus_gridftp_server_update_bytes_written(op,offset,nbytes);
        }
      }

      globus_free(buffer);
      /* if not done just register the next one */
      if(!CASTOR2xroot_handle->done) {
        globus_l_gfs_CASTOR2xroot_read_from_net(CASTOR2xroot_handle);
      }
      /* if done and there are no outstanding callbacks finish */
      else if(CASTOR2xroot_handle->outstanding == 0){
        XrdPosix_Close(CASTOR2xroot_handle->fd);
        globus_gridftp_server_finished_transfer
          (op, CASTOR2xroot_handle->cached_res);
      }
    }
    globus_mutex_unlock(&CASTOR2xroot_handle->mutex);
  }

  static void globus_l_gfs_CASTOR2xroot_read_from_net
  (globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle) {
    globus_byte_t *buffer;
    globus_result_t result;
    const char *func = "globus_l_gfs_CASTOR2xroot_read_from_net";

    GlobusGFSName(globus_l_gfs_CASTOR2xroot_read_from_net);
    /* in the read case this number will vary */
    globus_gridftp_server_get_optimal_concurrency
      (CASTOR2xroot_handle->op, &CASTOR2xroot_handle->optimal_count);

    while(CASTOR2xroot_handle->outstanding <
          CASTOR2xroot_handle->optimal_count) {
      buffer=(globus_byte_t*)globus_malloc(CASTOR2xroot_handle->block_size);
      if (buffer == NULL) {
        result = GlobusGFSErrorGeneric("error: globus malloc failed");
        CASTOR2xroot_handle->cached_res = result;
        CASTOR2xroot_handle->done = GLOBUS_TRUE;
        if(CASTOR2xroot_handle->outstanding == 0) {
          XrdPosix_Close(CASTOR2xroot_handle->fd);
          globus_gridftp_server_finished_transfer
            (CASTOR2xroot_handle->op, CASTOR2xroot_handle->cached_res);
        }
        return;
      }
      result= globus_gridftp_server_register_read(CASTOR2xroot_handle->op,
                                                  buffer,
                                                  CASTOR2xroot_handle->block_size,
                                                  globus_l_gfs_file_net_read_cb,
                                                  CASTOR2xroot_handle);

      if(result != GLOBUS_SUCCESS)  {
        globus_gfs_log_message
          (GLOBUS_GFS_LOG_ERR,
           "%s: register read has finished with a bad result \n",
           func);
        globus_free(buffer);
        CASTOR2xroot_handle->cached_res = result;
        CASTOR2xroot_handle->done = GLOBUS_TRUE;
        if(CASTOR2xroot_handle->outstanding == 0) {
          XrdPosix_Close(CASTOR2xroot_handle->fd);
          globus_gridftp_server_finished_transfer
            (CASTOR2xroot_handle->op, CASTOR2xroot_handle->cached_res);
        }
        return;
      }
      CASTOR2xroot_handle->outstanding++;
    }
  }

  static void
  globus_l_gfs_CASTOR2xroot_recv(globus_gfs_operation_t op,
                                 globus_gfs_transfer_info_t *transfer_info,
                                 void *user_arg) {
    globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle;
    globus_result_t result;
    const char *func = "globus_l_gfs_CASTOR2xroot_recv";
    char *pathname;
    int flags;

    GlobusGFSName(globus_l_gfs_CASTOR2xroot_recv);
    CASTOR2xroot_handle = (globus_l_gfs_CASTOR2xroot_handle_t *) user_arg;

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

    if(CASTOR2xroot_handle->use_uuid) {
      // we use uuid mode to access files
      if (strcmp(CASTOR2xroot_handle->access_mode,"w") != 0 &&
          strcmp(CASTOR2xroot_handle->access_mode,"o") != 0) {
        result=GlobusGFSErrorGeneric("error: incorrect access mode");
        globus_gridftp_server_finished_transfer(op, result);
        return;
      }
    }
    pathname=strdup(transfer_info->pathname);
    if (NULL == pathname) {
      result=GlobusGFSErrorGeneric("error: strdup failed");
      globus_gridftp_server_finished_transfer(op, result);
      return;
    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: pathname: %s \n",
                           func,pathname);

    // try to open
    flags = O_WRONLY | O_CREAT;
    if(transfer_info->truncate) flags |= O_TRUNC;

    CASTOR2xroot_handle->fd =
      CASTOR2xroot_handle_open(pathname, flags, 0644, CASTOR2xroot_handle);
    if (CASTOR2xroot_handle->fd < 0) {
      result=globus_l_gfs_make_error("open/create", errno);
      free(pathname);
      globus_gridftp_server_finished_transfer(op, result);
      return;
    }

    // reset all the needed variables in the handle
    CASTOR2xroot_handle->cached_res = GLOBUS_SUCCESS;
    CASTOR2xroot_handle->outstanding = 0;
    CASTOR2xroot_handle->done = GLOBUS_FALSE;
    CASTOR2xroot_handle->blk_length = 0;
    CASTOR2xroot_handle->blk_offset = 0;
    CASTOR2xroot_handle->op = op;

    globus_gridftp_server_get_block_size(op, &CASTOR2xroot_handle->block_size);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: block size: %ld\n",
                           func,CASTOR2xroot_handle->block_size);

    globus_gridftp_server_begin_transfer(op, 0, CASTOR2xroot_handle);

    globus_mutex_lock(&CASTOR2xroot_handle->mutex);
    {
      globus_l_gfs_CASTOR2xroot_read_from_net(CASTOR2xroot_handle);
    }
    globus_mutex_unlock(&CASTOR2xroot_handle->mutex);
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
  static void
  globus_l_gfs_CASTOR2xroot_send(globus_gfs_operation_t op,
                                 globus_gfs_transfer_info_t *transfer_info,
                                 void *user_arg) {
    globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle;
    const char *func = "globus_l_gfs_CASTOR2xroot_send";
    char *pathname;
    int i;
    globus_bool_t done;
    globus_result_t result;

    GlobusGFSName(globus_l_gfs_CASTOR2xroot_send);
    CASTOR2xroot_handle = (globus_l_gfs_CASTOR2xroot_handle_t *) user_arg;
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

    if (CASTOR2xroot_handle->use_uuid) {
      // we use uuid mode to access files
      if (strcmp(CASTOR2xroot_handle->access_mode,"r") != 0 &&
          strcmp(CASTOR2xroot_handle->access_mode,"o") != 0) {
        result=GlobusGFSErrorGeneric("error: incorect access mode");
        globus_gridftp_server_finished_transfer(op, result);
        return;
      }
    }

    pathname=strdup(transfer_info->pathname);
    if (pathname == NULL) {
      result = GlobusGFSErrorGeneric("error: strdup failed");
      globus_gridftp_server_finished_transfer(op, result);
      return;
    }

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: pathname: %s\n",
                           func,pathname);
    CASTOR2xroot_handle->fd =
      CASTOR2xroot_handle_open(pathname, O_RDONLY, 0, CASTOR2xroot_handle);   /* mode is ignored */

    if(CASTOR2xroot_handle->fd < 0) {
      result = globus_l_gfs_make_error("open", errno);
      free(pathname);
      globus_gridftp_server_finished_transfer(op, result);
      return;
    }

    /* reset all the needed variables in the handle */
    CASTOR2xroot_handle->cached_res = GLOBUS_SUCCESS;
    CASTOR2xroot_handle->outstanding = 0;
    CASTOR2xroot_handle->done = GLOBUS_FALSE;
    CASTOR2xroot_handle->blk_length = 0;
    CASTOR2xroot_handle->blk_offset = 0;
    CASTOR2xroot_handle->op = op;

    globus_gridftp_server_get_optimal_concurrency
      (op, &CASTOR2xroot_handle->optimal_count);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: optimal_concurrency: %u\n",
                           func,CASTOR2xroot_handle->optimal_count);

    globus_gridftp_server_get_block_size(op, &CASTOR2xroot_handle->block_size);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: block_size: %ld\n",
                           func,CASTOR2xroot_handle->block_size);

    globus_gridftp_server_begin_transfer(op, 0, CASTOR2xroot_handle);
    done = GLOBUS_FALSE;
    globus_mutex_lock(&CASTOR2xroot_handle->mutex);
    {
      for(i = 0; i < CASTOR2xroot_handle->optimal_count && !done; i++) {
        done = globus_l_gfs_CASTOR2xroot_send_next_to_client(CASTOR2xroot_handle);
      }
    }
    globus_mutex_unlock(&CASTOR2xroot_handle->mutex);
    free(pathname);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
  }

  static globus_bool_t
  globus_l_gfs_CASTOR2xroot_send_next_to_client
  (globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle) {
    globus_result_t result;
    globus_result_t res;
    globus_off_t read_length;
    globus_off_t nbread;
    globus_off_t start_offset;
    globus_byte_t *buffer;
    const char *func = "globus_l_gfs_CASTOR2xroot_send_next_to_client";

    GlobusGFSName(globus_l_gfs_CASTOR2xroot_send_next_to_client);

    if (CASTOR2xroot_handle->blk_length == 0) {
      // check the next range to read
      globus_gridftp_server_get_read_range(CASTOR2xroot_handle->op,
                                           &CASTOR2xroot_handle->blk_offset,
                                           &CASTOR2xroot_handle->blk_length);
      if (CASTOR2xroot_handle->blk_length == 0) {
        result = GLOBUS_SUCCESS;
        XrdPosix_Close(CASTOR2xroot_handle->fd);
        CASTOR2xroot_handle->cached_res = result;
        CASTOR2xroot_handle->done = GLOBUS_TRUE;
        if (CASTOR2xroot_handle->outstanding == 0) {
          globus_gridftp_server_finished_transfer
            (CASTOR2xroot_handle->op, CASTOR2xroot_handle->cached_res);
        }
        return CASTOR2xroot_handle->done;
      }
    }

    if (CASTOR2xroot_handle->blk_length == -1 ||
        CASTOR2xroot_handle->blk_length > (globus_off_t)CASTOR2xroot_handle->block_size) {
      read_length = CASTOR2xroot_handle->block_size;
    } else {
      read_length = CASTOR2xroot_handle->blk_length;
    }

    start_offset = XrdPosix_Lseek(CASTOR2xroot_handle->fd,
                                  CASTOR2xroot_handle->blk_offset,
                                  SEEK_SET);
    // verify that it worked
    if (start_offset != CASTOR2xroot_handle->blk_offset) {
      result = globus_l_gfs_make_error("seek", errno);
      XrdPosix_Close(CASTOR2xroot_handle->fd);
      CASTOR2xroot_handle->cached_res = result;
      CASTOR2xroot_handle->done = GLOBUS_TRUE;
      if (CASTOR2xroot_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(CASTOR2xroot_handle->op,
                                                CASTOR2xroot_handle->cached_res);
      }
      return CASTOR2xroot_handle->done;
    }

    buffer = (globus_byte_t*)globus_malloc(read_length);
    if(buffer == NULL) {
      result = GlobusGFSErrorGeneric("error: malloc failed");
      XrdPosix_Close(CASTOR2xroot_handle->fd);
      CASTOR2xroot_handle->cached_res = result;
      CASTOR2xroot_handle->done = GLOBUS_TRUE;
      if (CASTOR2xroot_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(CASTOR2xroot_handle->op,
                                                CASTOR2xroot_handle->cached_res);
      }
      return CASTOR2xroot_handle->done;
    }

    nbread = XrdPosix_Read(CASTOR2xroot_handle->fd, buffer, read_length);
    if (nbread == 0) { // eof
      result = GLOBUS_SUCCESS;
      globus_free(buffer);
      XrdPosix_Close(CASTOR2xroot_handle->fd);
      CASTOR2xroot_handle->cached_res = result;
      CASTOR2xroot_handle->done = GLOBUS_TRUE;
      if (CASTOR2xroot_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(CASTOR2xroot_handle->op,
                                                CASTOR2xroot_handle->cached_res);
      }
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: finished (eof)\n",func);
      return CASTOR2xroot_handle->done;
    }
    if (nbread < 0) { // error
      result = globus_l_gfs_make_error("read", errno);
      globus_free(buffer);
      XrdPosix_Close(CASTOR2xroot_handle->fd);
      CASTOR2xroot_handle->cached_res = result;
      CASTOR2xroot_handle->done = GLOBUS_TRUE;
      if (CASTOR2xroot_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(CASTOR2xroot_handle->op,
                                                CASTOR2xroot_handle->cached_res);
      }
      globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: finished (error)\n",func);
      return CASTOR2xroot_handle->done;

    }

    if (read_length>=nbread) {
      // if we have a file with size less than block_size we do not
      // have use parrallel connections (one will be enough)
      CASTOR2xroot_handle->optimal_count--;
    }
    read_length = nbread;

    if (CASTOR2xroot_handle->blk_length != -1) {
      CASTOR2xroot_handle->blk_length -= read_length;
    }

    // start offset?
    res = globus_gridftp_server_register_write(CASTOR2xroot_handle->op,
                                               buffer,
                                               read_length,
                                               CASTOR2xroot_handle->blk_offset,
                                               -1,
                                               globus_l_gfs_net_write_cb,
                                               CASTOR2xroot_handle);
    CASTOR2xroot_handle->blk_offset += read_length;
    if (res != GLOBUS_SUCCESS) {
      globus_free(buffer);
      XrdPosix_Close(CASTOR2xroot_handle->fd);
      CASTOR2xroot_handle->cached_res = res;
      CASTOR2xroot_handle->done = GLOBUS_TRUE;
      if (CASTOR2xroot_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(CASTOR2xroot_handle->op,
                                                CASTOR2xroot_handle->cached_res);
      }
      return CASTOR2xroot_handle->done;
    }

    CASTOR2xroot_handle->outstanding++;
    return GLOBUS_FALSE;
  }


  static void
  globus_l_gfs_net_write_cb(globus_gfs_operation_t op,
                            globus_result_t result,
                            globus_byte_t *buffer,
                            globus_size_t,
                            void * user_arg) {
    globus_l_gfs_CASTOR2xroot_handle_t *CASTOR2xroot_handle;
    const char *func = "globus_l_gfs_net_write_cb";

    CASTOR2xroot_handle = (globus_l_gfs_CASTOR2xroot_handle_t *) user_arg;

    globus_free(buffer);
    globus_mutex_lock(&CASTOR2xroot_handle->mutex);
    {
      CASTOR2xroot_handle->outstanding--;
      if (result != GLOBUS_SUCCESS) {
        CASTOR2xroot_handle->cached_res = result;
        CASTOR2xroot_handle->done = GLOBUS_TRUE;
      }
      if (!CASTOR2xroot_handle->done) {
        globus_l_gfs_CASTOR2xroot_send_next_to_client(CASTOR2xroot_handle);
      } else if (CASTOR2xroot_handle->outstanding == 0) {
        XrdPosix_Close(CASTOR2xroot_handle->fd);
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                               "%s: finished transfer\n",
                               func);
        globus_gridftp_server_finished_transfer
          (op, CASTOR2xroot_handle->cached_res);
      }
    }
    globus_mutex_unlock(&CASTOR2xroot_handle->mutex);
  }


  static int globus_l_gfs_CASTOR2xroot_activate(void);

  static int globus_l_gfs_CASTOR2xroot_deactivate(void);

  /// no need to change this
  static globus_gfs_storage_iface_t globus_l_gfs_CASTOR2xroot_dsi_iface =
  {
    GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | GLOBUS_GFS_DSI_DESCRIPTOR_SENDER,
    globus_l_gfs_CASTOR2xroot_start,
    globus_l_gfs_CASTOR2xroot_destroy,
    NULL, /* list */
    globus_l_gfs_CASTOR2xroot_send,
    globus_l_gfs_CASTOR2xroot_recv,
    NULL, /* trev */
    NULL, /* active */
    NULL, /* passive */
    NULL, /* data destroy */
    globus_l_gfs_CASTOR2xroot_command,
    globus_l_gfs_CASTOR2xroot_stat,
    NULL,
    NULL
  };


  /// no need to change this
  GlobusExtensionDefineModule(globus_gridftp_server_CASTOR2xroot) =
  {
    (char*)"globus_gridftp_server_CASTOR2xroot",
    globus_l_gfs_CASTOR2xroot_activate,
    globus_l_gfs_CASTOR2xroot_deactivate,
    NULL,
    NULL,
    &local_version,
    NULL
  };

  /// no need to change this
  static int globus_l_gfs_CASTOR2xroot_activate(void) {
    globus_extension_registry_add
      (GLOBUS_GFS_DSI_REGISTRY,
       (void*)"CASTOR2xroot",
       GlobusExtensionMyModule(globus_gridftp_server_CASTOR2xroot),
       &globus_l_gfs_CASTOR2xroot_dsi_iface);
    return 0;
  }

  /// no need to change this
  static int globus_l_gfs_CASTOR2xroot_deactivate(void) {
    globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, (void*)"CASTOR2xroot");

    return 0;
  }

} // end extern "C"
