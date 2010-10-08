/******************************************************************************
 *                      schmod_shmem.cpp
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
 * @(#)$RCSfile: schmod_shmem.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2009/07/23 12:21:59 $ $Author: waldron $
 *
 * Castor LSF External Plugin - Phase 1 (Shared Memory)
 *
 * This is the first of two LSF plugins for Castor and should be inserted after
 * the schmod_default plugin in the lsb.modules list in LSF. Its primary goal
 * is to filter out all diskservers/machines from being possible candidates to
 * LSF if they do not fulfil the necessary criteria for acceptance. By
 * eliminating machines at the very beginning of the processing stack we avoid
 * giving machines to LSF's other modules such as schmod_parallel therefore
 * reducing the processing time required.
 *
 * Note: The plugin deliberately doesn't use DLF logging. Any major errors
 *       will be logged into LSF's log file
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/scheduler/schmod_shmem.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/FileSystemStatus.hpp"
#include <errno.h>
#include <algorithm>

// Shared memory pointer
castor::monitoring::ClusterStatus *clusterStatus = 0;


extern "C" {

  //---------------------------------------------------------------------------
  // sched_init
  //---------------------------------------------------------------------------
  int sched_init(void*) {

    // Get pointer to the shared memory
    try {
      bool create = false;
      clusterStatus =
        castor::monitoring::ClusterStatus::getClusterStatus(create);
    } catch (castor::exception::Exception& e) {
      ls_syslog(LOG_ERR, "shmod_shmem: failed to access shared memory: %s",
                e.getMessage().str().c_str());
      return -1;
    }

    // Shared memory defined ?
    if (clusterStatus == NULL) {
      ls_syslog(LOG_ERR, "shmod_shmem: shared memory doesn't exist, check "
                "that the rmmaster daemon is running");
      return -1;
    }

    // Initialize plugin handler
    RsrcReqHandlerType *handler =
      (RsrcReqHandlerType *) calloc(1, sizeof(RsrcReqHandlerType));
    if (handler == NULL) {
      ls_syslog(LOG_ERR, "shmod_shmem: failed to allocate memory: %s",
                strerror(errno));
      return -1;
    }

    // Register plugin handler for resource requirement processing
    handler->newFn   = (RsrcReqHandler_NewFn) shmem_new;
    handler->matchFn = (RsrcReqHandler_MatchFn) shmem_match;
    handler->freeFn  = (RsrcReqHandler_FreeFn) shmem_delete;

    // Register the job resource requirement handle
    lsb_resreq_registerhandler(HANDLER_SHMEM_ID, handler);

    // LSF keeps a copy of the handler, so its safe to free it
    free(handler);

    return 0;
  }


  //---------------------------------------------------------------------------
  // sched_version
  //---------------------------------------------------------------------------
  int sched_version(void*) {
    return 0;
  }


  //---------------------------------------------------------------------------
  // shmem_new
  //---------------------------------------------------------------------------
  int shmem_new(void *resreq) {

    // Check parameters
    if (resreq == NULL) {
      return 0;
    }

    // Attempt to create a new HandlerData structure for the job
    castor::scheduler::HandlerData *handler = 0;
    try {
      handler = new castor::scheduler::HandlerData(resreq);

      // Set the key and handler specific data for this resource requirement
      lsb_resreq_setobject(resreq, HANDLER_SHMEM_ID,
                           (char *)handler->jobName.c_str(),
                           handler);
    } catch (castor::exception::Exception& e) {
      // Free resources
      if (handler)
        delete handler;
      return -1;
    }

    return 0;
  }


  //---------------------------------------------------------------------------
  // shmem_match
  //---------------------------------------------------------------------------
  int shmem_match(castor::scheduler::HandlerData *handler,
                  void *candGroupList,
                  void *reasonTb) {

    // Check parameters
    if (handler == NULL) {
      return 0;
    }
    if ((candGroupList == NULL) || (reasonTb == NULL)) {
      return -1;
    }

    // Loop over the list of candidate hosts LSF has provided and eliminate
    // those that do not fulfil the necessary requirements
    candHostGroup *candGroupEntry = lsb_cand_getnextgroup(candGroupList);
    while (candGroupEntry != NULL) {
      int index = 0, reason = 0;
      while (index < candGroupEntry->numOfMembers) {
        candHost *candHost = &(candGroupEntry->candHost[index]);
        hostSlot *hInfo    = lsb_cand_getavailslot(candHost);

        // Cast normal string into sharedMemory one in order to be able to
        // search for it in the clusterStatus map.
        std::string diskServer = hInfo->name;
        castor::monitoring::SharedMemoryString *smDiskServer =
          (castor::monitoring::SharedMemoryString *)(&(diskServer));
        castor::monitoring::ClusterStatus::const_iterator it =
          clusterStatus->find(*smDiskServer);

        // If the job has explicitly defined a list of requested filesystems,
        // exclude any diskserver not in the list.
        std::vector<std::string>::const_iterator it2 =
          std::find(handler->rfs.begin(), handler->rfs.end(), diskServer);

        // Check to see if the diskserver is in the exclusion host list
        std::vector<std::string>::const_iterator it4 =
          std::find(handler->excludedHosts.begin(),
                    handler->excludedHosts.end(), diskServer);

        reason = 0;
        if (handler->rfs.size() && (it2 == handler->rfs.end())) {
          reason = PEND_HOST_CNOTRFS;  // Diskserver is not in the list of RFS
        }
        else if (it == clusterStatus->end()) {
          reason = PEND_HOST_CUNKNOWN; // Host not listed in shared memory
        }
        else if ((handler->requestType ==
                  castor::OBJ_StageDiskCopyReplicaRequest) &&
                 (handler->sourceDiskServer != diskServer) &&
                 (it4 != handler->excludedHosts.end())) {
          reason = PEND_HOST_CEXCLUDE; // Diskserver in exclusion list
        }

        // For non diskcopy replication requests the diskserver must be in
        // PRODUCTION
        else if ((handler->requestType !=
                  castor::OBJ_StageDiskCopyReplicaRequest) &&
                 (it->second.status() !=
                  castor::stager::DISKSERVER_PRODUCTION)) {
          reason = PEND_HOST_CSTATE;   // Diskserver status incorrect
        }

        // For diskcopy replication requests where the diskserver being
        // processed is the source diskserver the diskserver must be in
        // PRODUCTION or DRAINING
        else if ((handler->requestType ==
                  castor::OBJ_StageDiskCopyReplicaRequest) &&
                 (handler->sourceDiskServer == diskServer) &&
                 (it->second.status() ==
                  castor::stager::DISKSERVER_DISABLED)) {
          reason = PEND_HOST_CSTATE;   // Diskserver status incorrect
        }

        // For diskcopy replication requests where the diskserver being
        // processed is not the source diskserver. I.e it could be the
        // destination end of the transfer the diskserver must be in
        // PRODUCTION
        else if ((handler->requestType ==
                  castor::OBJ_StageDiskCopyReplicaRequest) &&
                 (handler->sourceDiskServer != diskServer) &&
                 (it->second.status() !=
                  castor::stager::DISKSERVER_PRODUCTION)) {
          reason = PEND_HOST_CSTATE;   // Diskserver status incorrect
        }

        // Up to this point we have only processed information about the
        // diskserver as a whole entity. Now we must look at its filesystems
        // to determine if they are ok. We only look at status here not space!
        else {
          unsigned long disabled = 0;
          for (castor::monitoring::DiskServerStatus::const_iterator it3 =
                 it->second.begin();
               it3 != it->second.end(); it3++) {

            // For non diskcopy replication requests the filesystem must be in
            // PRODUCTION
            if ((handler->requestType !=
                 castor::OBJ_StageDiskCopyReplicaRequest) &&
                (it3->second.status() !=
                 castor::stager::FILESYSTEM_PRODUCTION)) {
              disabled++;
            }

            // For diskcopy replication requests where the diskserver being
            // processed is the source diskserver the diskserver must be in
            // PRODUCTION or DRAINING
            else if ((handler->requestType ==
                      castor::OBJ_StageDiskCopyReplicaRequest) &&
                     (handler->sourceDiskServer == diskServer)) {
              if ((handler->sourceFileSystem == it3->first.c_str()) &&
                  (it3->second.status() ==
                   castor::stager::FILESYSTEM_DISABLED)) {
                disabled++;

                // Fail this diskserver completely. Diskcopy replication
                // requests force a first execution host within a parallel
                // job submission. If the filesystem with the source file is
                // unavailable then the job is not eligible to run.
                reason = PEND_HOST_CSTATE;
              }
            }

            // For all other requests the filesystem must be in PRODUCTION
            else if (it3->second.status() !=
                     castor::stager::FILESYSTEM_PRODUCTION) {
              disabled++;
            }
          }

          // Only remove the diskserver if all its filesystems are disabled
          if (disabled == it->second.size()) {
            reason = PEND_HOST_CSTATE; // Diskserver status incorrect
          }
        }

        // Remove the host from the candidate list if a reason exists
        if (reason) {
          lsb_reason_set(reasonTb, candHost, reason);
          lsb_cand_removehost(candGroupEntry, index);
        } else {
          index++;
        }
        free(hInfo);
      }
      candGroupEntry = lsb_cand_getnextgroup(NULL);
    }

    return 0;
  }


  //---------------------------------------------------------------------------
  // shmem_delete
  //---------------------------------------------------------------------------
  void shmem_delete(castor::scheduler::HandlerData *handler) {
    if (handler != NULL) {
      delete handler;
    }
  }

} // End of extern "C"
