/******************************************************************************
 *                      schmod_python.cpp
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
 * @(#)$RCSfile: schmod_python.cpp,v $ $Revision: 1.12 $ $Release$ $Date: 2009/08/18 09:42:54 $ $Author: waldron $
 *
 * Castor LSF External Plugin - Phase 2 (Python)
 *
 * This is the second of two LSF plugins for Castor and should be the very
 * last plugin in the lsb.modules list in LSF. Its primary goal is to a select
 * a suitable filesystem where a job could run using python based policies.
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/scheduler/schmod_python.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/monitoring/FileSystemRating.hpp"
#include "castor/dlf/Dlf.hpp"
#include "getconfent.h"
#include "Cdlopen_api.h"
#include "Cthread_api.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <signal.h>
#include <float.h>
#include <fstream>
#include <algorithm>
#include <ios>

// Shared memory pointer
castor::monitoring::ClusterStatus *clusterStatus = 0;

// Python pointer
castor::scheduler::Python *python = 0;

// Configuration options
std::string notifyDir        = DEFAULT_NOTIFY_DIR;
std::string policyFile       = DEFAULT_POLICY_FILE;
std::string dynamicPythonLib = DEFAULT_DYNAMIC_PYTHON_LIB;

// Coefficients for the python_update_deltas function
int jobReadRateCost          = 1000;
int jobWriteRateCost         = 1000;
int jobNbReadStreamCost      = 1;
int jobNbReadWriteStreamCost = 1;
int jobNbWriteStreamCost     = 1;


extern "C" {

  //---------------------------------------------------------------------------
  // sched_init
  //---------------------------------------------------------------------------
  int sched_init(void*) {

    signal(SIGPIPE, SIG_IGN);

    // Initialize DLF logging
    try {
      castor::dlf::Message messages[] = {
        {  0, "LSF plugin initialization started" },
        {  1, "Unable to access shared memory" },
        {  2, "Shared memory doesn't exist, check that the rmmaster daemon is running" },
        {  3, "Failed to initialize handler plugin" },
        {  4, "Unable to allocate SharedMemoryString" },
        {  6, "Sched/SharedLSFResource configuration option not defined" },
        {  7, "Failed to initialise LSF batch library interface" },
        {  8, "Failed to open Sched/SharedLSFResource directory" },
        {  9, "Failed to determine the service classes of all diskservers" },
        { 10, "LSF plugin initialization successful" },
        { 11, "Failed to create the ratings thread to analyse diskserver and filesystem information" },
        { 13, "Failed to parse resource requirements, exiting castor_new" },
        { 14, "Failed to load python library" },
        { 15, "Duplicate job detected. A job with the same name is already registered in the plugin" },
        { 23, "Error caught rating a filesystem" },
        { 33, "Failed to open notification file" },
        { 34, "Wrote notification file" },
        { 35, "LSF Job already notified, skipping notification phase" },
        { 80, "Missing handler specific data in notification phase" },
        { 82, "Exception caught in initializing embedded Python interpreter" },
        { -1, "" }};
      castor::dlf::dlf_init("schedulerd", messages);
    } catch (castor::exception::Exception e) {
      // Ignored. DLF failure is not a reason to stop the module loading
    }

    // "LSF plugin initialization started"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Plugin", "schmod_python")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 0, 1, params);

    // Get the value of the SharedLSFResource. The plugin will write job
    // notification files in this directory which will be read/downloaded by
    // the remote execution job in order to instruct the job on which
    // filesystem to use.
    char *value = getconfent("Sched", "SharedLSFResource", 0);
    if (value == NULL) {
      // "Sched/SharedLSFResource configuration option not defined"
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6, 0, 0);
      return -1;
    }
    notifyDir = value;

    // Strip trailing '/' from the notification directory path. This is purely
    // for cosmetic reasons.
    if (notifyDir[notifyDir.length() - 1] == '/') {
      notifyDir = notifyDir.substr(0, notifyDir.length() - 1);
    }

    // Initialize coefficients
    python_init_coeffs();

    // We need to cleanup the notification directory here to prevent orphaned
    // notification files from collecting after the module reloads. During a
    // reload a job can end without going through the deletion phase leaving
    // files behind. To prevent this we remove files older then 300 seconds
    DIR *dir = opendir(notifyDir.c_str());
    if (dir == NULL) {
      // "Failed to open Sched/SharedLSFResource directory"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Directory", notifyDir),
         castor::dlf::Param("Error", strerror(errno))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 8, 2, params);
      return -1;
    }

    // Loop over directory contents
    dirent *entry;
    while ((entry = readdir(dir))) {
      std::ostringstream filepath;
      filepath << notifyDir << "/" << entry->d_name;

      struct stat file;
      if (stat(filepath.str().c_str(), &file) < 0) {
        continue; // Ignored
      } else if (strspn(entry->d_name, "0123456789") !=
                 strlen(entry->d_name)) {
        continue; // A regular file with only numbers in the name
      } else if (!(file.st_mode & S_IFREG)) {
        continue; // Not a regular file
      } else if (file.st_mtime < (time(NULL) - 400)) {
        unlink(filepath.str().c_str());
      }
    }
    closedir(dir);

    // Get the location of the python policy module
    value = getconfent("Sched", "PolicyModule", 0);
    if (value != NULL) {
      policyFile = value;
    }

    // Load the python library.
    value = getconfent("Sched", "DynamicPythonLib", 0);
    if (value != NULL) {
      dynamicPythonLib = value;
    }
    void *handle = Cdlopen(value, RTLD_NOW | RTLD_GLOBAL);
    if (handle == NULL) {
      // "Failed to load python library"
      castor::dlf::Param params[] =
        {castor::dlf::Param("DynamicPythonLib", value),
         castor::dlf::Param("Error", Cdlerror())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 2, params);
      return -1;
    }

    // Get pointer to the shared memory
    try {
      bool create = false;
      clusterStatus =
        castor::monitoring::ClusterStatus::getClusterStatus(create);
    } catch (castor::exception::Exception e) {
      // "Unable to access shared memory"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", sstrerror(e.code())),
         castor::dlf::Param("Error", e.getMessage().str()),
         castor::dlf::Param("Plugin", "shmod_python")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1, 3, params);
      return -1;
    }

    // Shared memory defined ?
    if (clusterStatus == NULL) {
      // "Shared memory doesn't exist, check that the rmmaster daemon is
      //  running"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Plugin", "schmod_python")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
      return -1;
    }

    // In castor each filesystem must be associated with one and only one
    // diskpool which in turn can be associated with many service classes. LSF
    // itself does not support scheduling at a filesystem granularity. So it
    // is mandatory that all filesystems on a given diskserver belong to the
    // same diskpool and hence also the same service classes. You cannot split
    // filesystems on the same diskserver across different diskpools if those
    // diskpools do not share the same service classes in common.
    //
    // In order for the filesystem ratings to be calculated in the rating
    // thread all service classes associated with a filesystem must be known
    // in the shared memory in order to apply the correct python policy.
    //
    // A filesystem which belongs to service class A only will have 3 rating
    // values (read, write and read-write). A filesystem belonging to service
    // class A and B will have 6 rating values (read, write, read-write) for
    // each service class. `rmGetNodes -a`
    //
    // In LSF a diskserver can belong to one or more host groups. These host
    // groups map directly onto service classes. So we query LSF to determine
    // which services classes each diskserver and its associated filesystems
    // belong too and create the appropriate data structures in the shared
    // memory to reflect this.

    if (lsb_init((char*)"schmod_python") < 0) {
      // "Failed to initialise LSF batch library interface"
      castor::dlf::Param params[] =
             {castor::dlf::Param("Function", "lsb_init"),
             castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 2, params);
      return -1;
    }

    // Set all rating groups to inactive. If we don't do this then diskservers
    // cannot change from one service class to another without a complete
    // reset/wipe of the shared memory
    for (castor::monitoring::ClusterStatus::iterator it =
           clusterStatus->begin(); it != clusterStatus->end(); it++) {
      for (castor::monitoring::DiskServerStatus::iterator it2 =
             it->second.begin(); it2 != it->second.end(); it2++) {
        for (castor::monitoring::FileSystemStatus::iterator it3 =
               it2->second.begin(); it3 != it2->second.end(); it3++) {
          it3->second.setActive(false);
        }
      }
    }

    // Query LSF for a list of all host groups and their associated members.
    // The success of this call is mandatory unlike the previous
    // lsb_openjobinfo call which could be overlooked upon failure
    int enumGrp = 0;
    groupInfoEnt *grpInfo = NULL;
    if ((grpInfo = lsb_hostgrpinfo(NULL, &enumGrp, HOST_GRP)) == NULL) {
      // "Failed to determine the service classes of all diskservers"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "lsb_hostgrpinfo"),
         castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 9, 2, params);
      return -1;
    }

    // Loop over result set
    int i;
    for (i = 0; i < enumGrp; i++) {

      // Extract the real group name. The format is groupName<Group>
      std::string hostGroup(grpInfo[i].group);
      std::string::size_type index =
        hostGroup.find_last_of("Group", hostGroup.length());
      if (index == std::string::npos) {
        continue;   // Group name does not conform to known standards
      }
      std::string groupName = hostGroup.substr(0, index - 4);

      // Loop over all diskservers which are members of this group
      std::istringstream membersList(grpInfo[i].memberList);
      std::string diskServer;
      while (std::getline(membersList, diskServer, ' ')) {

        // Cast normal string into sharedMemory one in order to be able to
        // search for it in the clusterStatus map.
        castor::monitoring::SharedMemoryString *smDiskServer =
          (castor::monitoring::SharedMemoryString *)(&(diskServer));
        castor::monitoring::ClusterStatus::iterator it =
          clusterStatus->find(*smDiskServer);
        if (it == clusterStatus->end()) {
          continue; // Not yet registered with the rmmaster daemon
        }

        // Loop over all filesystems. Check if the rating group already exists.
        // If so activate it or create and associate a new rating group with
        // the filesystem
        for (castor::monitoring::DiskServerStatus::iterator it2 =
               it->second.begin();
             it2 != it->second.end();
             it2++) {

          // Cast normal string into sharedMemory one in order to be able to
          // search for it in the fileSystemStatus map.
          castor::monitoring::SharedMemoryString *smGroupName =
            (castor::monitoring::SharedMemoryString *)(&(groupName));
          castor::monitoring::FileSystemStatus::iterator it3 =
            it2->second.find(*smGroupName);

          if (it3 == it2->second.end()) {
            try {
              // Here we really have to create a shared memory string, the
              // previous cast won't work since we need to allocate memory
              castor::monitoring::SharedMemoryString smGName(groupName.c_str());
              it2->second.insert
                (std::make_pair(smGName,
                                castor::monitoring::FileSystemRating())).first;
            } catch (castor::exception::Exception e) {
              // "Unable to allocate SharedMemoryString"
              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 0, 0);
              return -1;
            }
          }
          // Flag the group as valid and active
          it3->second.setActive(true);
        }
      }
    }

    // Initialize plugin handler
    RsrcReqHandlerType *handler =
      (RsrcReqHandlerType *) calloc(1, sizeof(RsrcReqHandlerType));
    if (handler == NULL) {
      // "Failed to initialize handler plugin"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", strerror(errno)),
         castor::dlf::Param("Plugin", "schmod_python")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 2, params);
      return -1;
    }

    // Register plugin handler for resource requirement processing
    handler->newFn         = (RsrcReqHandler_NewFn) python_new;
    handler->matchFn       = (RsrcReqHandler_MatchFn) python_match;
    handler->notifyAllocFn = (RsrcReqHandler_NotifyAllocFn) python_notify;
    handler->freeFn        = (RsrcReqHandler_FreeFn) python_delete;

    // Register the job resource requirement handle
    lsb_resreq_registerhandler(HANDLER_PYTHON_ID, handler);

    // LSF keeps a copy of the handler, so its safe to free it
    free(handler);

    // Initialize python
    try {
      python = new castor::scheduler::Python(policyFile);
    } catch (castor::exception::Exception e) {
      // Exception caught in initializing embedded Python interpreter"
      castor::dlf::Param params[] =
        {castor::dlf::Param("PolicyFile", policyFile),
         castor::dlf::Param("Type", sstrerror(e.code())),
         castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 82, 3, params);
      return -1;
    }

    // Create the ratings thread used to calculate the rating of all
    // filesystems in the shared memory in an asynchronous way.
    if (Cthread_create((void *(*)(void *))python_rate, NULL) < 0) {
      // "Failed to create the ratings thread to analyse diskserver and
      // filesystem information"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", sstrerror(serrno))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 11, 1, params);
      return -1;
    }

    // "LSF plugin initialization successful"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("PolicyFile", policyFile),
       castor::dlf::Param("SharedLSFResource", notifyDir),
       castor::dlf::Param("PythonVersion", python->version()),
       castor::dlf::Param("Plugin", "schmod_python")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 10, 4, params2);

    return 0;
  }


  //---------------------------------------------------------------------------
  // sched_version
  //---------------------------------------------------------------------------
  int sched_version(void*) {
    return 0;
  }


  //---------------------------------------------------------------------------
  // python_init_coeffs
  //---------------------------------------------------------------------------
  void python_init_coeffs() {

    // Load estimation when a new job starts
    castor::scheduler::getIntCoeff
      (jobReadRateCost, "JobReadRateCost");
    castor::scheduler::getIntCoeff
      (jobWriteRateCost, "JobWriteRateCost");
    castor::scheduler::getIntCoeff
      (jobNbReadStreamCost, "JobNbReadStreamCost");
    castor::scheduler::getIntCoeff
      (jobNbReadWriteStreamCost, "JobNbReadWriteStreamCost");
    castor::scheduler::getIntCoeff
      (jobNbWriteStreamCost, "JobNbWriteStreamCost");
  }


  //---------------------------------------------------------------------------
  // python_new
  //---------------------------------------------------------------------------
  int python_new(void *resreq) {

    // Check parameters
    if (resreq == NULL) {
      return 0;
    }

    // Attempt to create a new HandlerData structure for the job
    castor::scheduler::HandlerData *handler = 0;
    try {
      handler = new castor::scheduler::HandlerData(resreq);

      // Add the handler data to the internal hash table. But first check that
      // a job with the same name isn't already registered!
      castor::scheduler::HandlerData *it =
        castor::scheduler::hashTable[handler->jobName.c_str()];
      if (it != NULL) {

        // "Duplicate job detected. A job with the same name is already
        // registered in the plugin"
        castor::dlf::Param params[] =
          {castor::dlf::Param("JobName", handler->jobName),
           castor::dlf::Param(handler->subReqId)};
        castor::dlf::dlf_writep(handler->reqId, DLF_LVL_ERROR, 15, 2, params,
                                &handler->fileId);
        delete handler;
        return -1;
      }
      castor::scheduler::hashTable[handler->jobName.c_str()] = handler;

      // Set the key and handler specific data for this resource requirement
      lsb_resreq_setobject(resreq, HANDLER_PYTHON_ID,
                           (char *)handler->jobName.c_str(),
                           handler);
    } catch (castor::exception::Exception e) {
      // Free resources
      if (handler)
        delete handler;

      // "Failed to parse resource requirements, exiting python_new"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", sstrerror(e.code())),
         castor::dlf::Param("Error", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 2, params);
      return -1;
    }

    return 0;
  }


  //---------------------------------------------------------------------------
  // python_match
  //---------------------------------------------------------------------------
  int python_match(castor::scheduler::HandlerData *handler,
                   void *candGroupList,
                   void *reasonTb) {

    // Check parameters
    if (handler == NULL) {
      return 0;
    }
    if ((candGroupList == NULL) || (reasonTb == NULL)) {
      return -1;
    }

    // Useful variables
    float bestWeight = -FLT_MAX;
    bool validSource = true;

    // Reset the decision made by the previous run
    handler->selectedDiskServer = "";
    handler->selectedFileSystem = "";

    // Increment the number of matches performed on this job
    handler->matches++;

    // Loop over the list of candidate hosts LSF has provided and eliminate
    // those that do not fulfil the necessary requirements. The logic here
    // is almost duplicated from the schmod_shmem plugin but with all the
    // diskserver checks removed. Unfortunately this duplication cannot be
    // avoided!
    candHostGroup *candGroupEntry = lsb_cand_getnextgroup(candGroupList);
    while (candGroupEntry != NULL) {
      if (candGroupEntry->numOfMembers <= 0) {
        return 0; // No candidate hosts provided
      }
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

        reason = 0;
        if (it == clusterStatus->end()) {
          reason = PEND_HOST_CUNKNOWN; // Host not listed in shared memory
        } else if (python == NULL) {
          reason = PEND_HOST_PYERR;    // Embedded python interpreter error
        } else {

          // Loop over all filesystems
          for (castor::monitoring::DiskServerStatus::const_iterator it3 =
                 it->second.begin();
               it3 != it->second.end();
               it3++) {

            // If we have requested filesystems defined, check that this
            // filesystem is of interest to us
            if (handler->rfs.size()) {
              std::ostringstream key;
              key << diskServer << ":" << it3->first;
              std::vector<std::string>::const_iterator it2 =
                std::find(handler->rfs.begin(), handler->rfs.end(), key.str());
              if (it2 == handler->rfs.end()) {
                continue;
              }
            }

            // For non diskcopy replication requests the filesystem must be in
            // PRODUCTION
            else if ((handler->requestType !=
                      castor::OBJ_StageDiskCopyReplicaRequest) &&
                     (it3->second.status() !=
                      castor::stager::FILESYSTEM_PRODUCTION)) {
              continue;
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
                continue;
              }
            }

            // For all other requests the filesystem must be in PRODUCTION
            else if (it3->second.status() !=
                     castor::stager::FILESYSTEM_PRODUCTION) {
              continue;
            }

            // Does the filesystem have enough space to accept the job? This
            // check does not apply to diskcopy replication requests where
            // the source diskserver and filesystem are the ones being
            // tested. Why? because the job only reads from the source!
            if ((handler->xsize > 0) &&
                (!((handler->requestType ==
                    castor::OBJ_StageDiskCopyReplicaRequest) &&
                   (handler->sourceDiskServer == diskServer) &&
                   (handler->sourceFileSystem == it3->first.c_str())))) {

              // Calculate the actual amount of free space left on the
              // filesystem. To try and minimise the likelihood of writing
              // to a full filesystem we also try to take into consideration
              // how much the current number of streams will take up on the
              // filesystem in the not too distant future.
              signed64 actualFreeSpace =
                (signed64)it3->second.freeSpace() + it3->second.deltaFreeSpace() -
                handler->defaultFileSize * (it3->second.nbWriteStreams() +
                                            it3->second.deltaNbWriteStreams() +
                                            it3->second.nbReadWriteStreams() +
                                            it3->second.deltaNbReadWriteStreams());

              if (!((actualFreeSpace > (signed64)handler->xsize) &&
                    (actualFreeSpace > it3->second.minAllowedFreeSpace() *
                     it3->second.space()))) {
                continue;
              }
            }

            // At this point the filesystem is a possible candidate to run the
            // job. I.e. it has the correct status, has been requested and has
            // sufficient space. Now we look at the filesystems rating for the
            // requested service class.
            if ((handler->requestType ==
                 castor::OBJ_StageDiskCopyReplicaRequest) &&
                (handler->sourceDiskServer == diskServer) &&
                (handler->sourceFileSystem == it3->first.c_str())) {

              // Lookup the filesystem rating for the source filesystem that
              // a diskcopy replication request is to read from
              castor::monitoring::SharedMemoryString *smRatingGroup =
                (castor::monitoring::SharedMemoryString *)(&(handler->sourceSvcClass));
              castor::monitoring::FileSystemStatus::const_iterator it4 =
                it3->second.find(*smRatingGroup);
              if (it4 == it3->second.end()) {
                // The source diskserver is not in the service class we expected
                validSource = false;
              } else if (!it4->second.active()) {
                validSource = false;
              } else if (it4->second.readRating() > 0) {
                // Policies dictate that the filesystem cannot be read from
                validSource = false;
              }
            } else {

              // Lookup the filesystem rating for the service class
              castor::monitoring::SharedMemoryString *smRatingGroup =
                (castor::monitoring::SharedMemoryString *)(&(handler->svcClass));
              castor::monitoring::FileSystemStatus::const_iterator it4 =
                it3->second.find(*smRatingGroup);
              if (it4 == it3->second.end()) {
                continue; // Filesystem not part of service class?
              } else if (!it4->second.active()) {
                continue;
              }

              float newWeight = 0.0;
              if (handler->openFlags == "r") {
                newWeight = it4->second.readRating() -
                  it3->second.deltaReadRate() -
                  it3->second.deltaNbReadStreams();
              } else if (handler->openFlags == "w") {
                newWeight = it4->second.writeRating() -
                  it3->second.deltaWriteRate() -
                  it3->second.deltaNbWriteStreams();
              } else {
                newWeight = it4->second.readWriteRating() -
                  it3->second.deltaReadRate() -
                  it3->second.deltaWriteRate() -
                  it3->second.deltaNbReadWriteStreams();
              }

              if ((newWeight < 0) && (newWeight > bestWeight)) {
                handler->selectedDiskServer = diskServer;
                handler->selectedFileSystem = it3->first.c_str();
                bestWeight = newWeight;
              }
            }
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

    // At this point we have made our final decision on which diskserver and
    // filesystem to use so we notify LSF of the result and increase the
    // deltas for our choice in the shared memory
    candGroupEntry = lsb_cand_getnextgroup(candGroupList);
    while (candGroupEntry != NULL) {
      int index = 0;
      for (index = 0; index < candGroupEntry->numOfMembers; ) {
        candHost *candHost = &(candGroupEntry->candHost[index]);
        hostSlot *hInfo    = lsb_cand_getavailslot(candHost);

        // Useful variables
        std::string diskServer = handler->selectedDiskServer;
        std::string fileSystem = handler->selectedFileSystem;
        std::string openFlags  = handler->openFlags;

        // Determine if the diskserver should be removed
        bool remove = false;
        if ((handler->requestType ==
             castor::OBJ_StageDiskCopyReplicaRequest) &&
            (handler->sourceDiskServer == hInfo->name)) {
          if (validSource == false) {
            remove = true;
          } else {
            diskServer = handler->sourceDiskServer;
            fileSystem = handler->sourceFileSystem;
            openFlags  = "r";
          }
        } else if ((handler->selectedDiskServer != hInfo->name) ||
                   (validSource == false)) {
          remove = true;
        }

        if (remove) {
          lsb_reason_set(reasonTb, candHost, PEND_HOST_CNOINTEREST);
          lsb_cand_removehost(candGroupEntry, index);
        } else {
          // This diskserver and filesystem combination is to be kept so we
          // update the delta values in the shared memory
          python_update_deltas
            (diskServer, fileSystem, openFlags, handler->xsize);
          index++;
        }
        free(hInfo);
      }
      candGroupEntry = lsb_cand_getnextgroup(NULL);
    }

    return 0;
  }


  //---------------------------------------------------------------------------
  // python_update_deltas
  //---------------------------------------------------------------------------
  void python_update_deltas(std::string diskServer,
                            std::string fileSystem,
                            std::string openFlags,
                            u_signed64 fileSize) {

    // Find the diskserver
    castor::monitoring::SharedMemoryString *smDiskServer =
      (castor::monitoring::SharedMemoryString *)(&(diskServer));
    castor::monitoring::ClusterStatus::iterator it =
      clusterStatus->find(*smDiskServer);
    if (it == clusterStatus->end()) {
      return; // No diskserver found
    }

    // Find the filesystem
    castor::monitoring::SharedMemoryString *smFilesystem =
      (castor::monitoring::SharedMemoryString *)(&(fileSystem));
    castor::monitoring::DiskServerStatus::iterator it2 =
      it->second.find(*smFilesystem);
    if (it2 == it->second.end()) {
      return; // No filesystem found
    }

    // Update deltas
    if (openFlags == "r") {
      it->second.setDeltaReadRate
        (it->second.deltaReadRate() + jobReadRateCost);
      it->second.setDeltaNbReadStreams
        (it->second.deltaNbReadStreams() + jobNbReadStreamCost);
      it2->second.setDeltaReadRate
        (it2->second.deltaReadRate() + jobReadRateCost);
      it2->second.setDeltaNbReadStreams
        (it2->second.deltaNbReadStreams() + jobNbReadStreamCost);
    } else if (openFlags == "w") {
      it->second.setDeltaWriteRate
        (it->second.deltaWriteRate() + jobWriteRateCost);
      it->second.setDeltaNbWriteStreams
        (it->second.deltaNbWriteStreams() + jobNbWriteStreamCost);
      it2->second.setDeltaWriteRate
        (it2->second.deltaWriteRate() + jobWriteRateCost);
      it2->second.setDeltaNbWriteStreams
        (it2->second.deltaNbWriteStreams() + jobNbWriteStreamCost);
      it2->second.setDeltaFreeSpace(it2->second.deltaFreeSpace() - fileSize);
    } else {
      it->second.setDeltaWriteRate
        (it->second.deltaWriteRate() + jobWriteRateCost);
      it2->second.setDeltaWriteRate
        (it2->second.deltaWriteRate() + jobWriteRateCost);
      it->second.setDeltaReadRate
        (it->second.deltaReadRate() + jobReadRateCost);
      it2->second.setDeltaReadRate
        (it2->second.deltaReadRate() + jobReadRateCost);
      it->second.setDeltaNbReadWriteStreams
        (it->second.deltaNbReadWriteStreams() + jobNbReadWriteStreamCost);
      it2->second.setDeltaNbReadWriteStreams
        (it2->second.deltaNbReadWriteStreams() + jobNbReadWriteStreamCost);
      it2->second.setDeltaFreeSpace(it2->second.deltaFreeSpace() - fileSize);
    }

    // Reset the last time the filesystem was rated. This will force the
    // rating thread to re-evaluate it
    it2->second.setLastRatingUpdate(0);
  }


  //---------------------------------------------------------------------------
  // python_notify
  //---------------------------------------------------------------------------
  int python_notify(void *info,
                    void *job,
                    void *alloc,
                    void *allocLimitList,
                    int  notifyFlag) {

    // Check parameters
    if ((info == NULL)  || (job == NULL) ||
        (alloc == NULL) || (allocLimitList == NULL)) {
      return -1;
    }

    // Get the job information
    jobInfo *j = lsb_get_jobinfo(job, PHASE_NOTIFY);

    // Retrieve the handler key associated with this job from the resKey in
    // the jobInfo struct. We have to parse the string ourselves as no
    // convenient method exists!
    std::string buf(j->resKey);
    std::string::size_type index = buf.rfind("#");
    if (index == std::string::npos) {
      return -1; // Not a castor job
    }

    // Lookup handler specific data
    std::string key = buf.substr(index + 1, buf.length()).c_str();
    castor::scheduler::HandlerData *handler =
      castor::scheduler::hashTable[key.c_str()];
    if (handler == NULL) {
      // "Missing handler specific data in notification phase"
      castor::dlf::Param params[] =
        {castor::dlf::Param("HashKey", key),
         castor::dlf::Param("JobId", j->jobId),
         castor::dlf::Param("JobStatus", j->status)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 80, 3, params);
      return -1;
    }

    // Update the handler with LSF job information
    handler->jobId = j->jobId;

    // From this point forward we are only interested in the allocation
    // phase of the notification
    if ((notifyFlag & NOTIFY_ALLOC) != NOTIFY_ALLOC) {
      return 0;
    }

    // A lack of a diskserver or filesystem is an indication that the job has
    // either already been notified and currently running or the plugin has
    // just been reloaded
    if ((handler->selectedDiskServer == "") ||
        (handler->selectedFileSystem == "")) {
      // "LSF Job already notified, skipping notification phase"
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", handler->jobId),
         castor::dlf::Param("JobStatus", j->status),
         castor::dlf::Param(handler->subReqId)};
      castor::dlf::dlf_writep(handler->reqId, DLF_LVL_DEBUG, 35, 3, params,
                              &handler->fileId);
      return 0;
    }

    // Write the notification file
    std::ostringstream filepath;
    filepath << notifyDir << "/" << handler->jobId;
    handler->notifyFile = filepath.str();

    std::fstream fin(handler->notifyFile.c_str(),
                     std::ios::out | std::ios::trunc);
    if (!fin) {
      // "Failed to open notification file"
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", handler->jobId),
         castor::dlf::Param("Filename", handler->notifyFile),
         castor::dlf::Param(handler->subReqId)};
      castor::dlf::dlf_writep(handler->reqId, DLF_LVL_ERROR, 33, 3, params,
                              &handler->fileId);
      return -1;
    }

    // Write decision to file
    std::ostringstream ipath;
    ipath << handler->selectedDiskServer << ":" << handler->selectedFileSystem;
    fin << ipath.str() << std::endl;
    fin.close();

    // Calculate statistics. Note the submission time of the job is only
    // measured to the accuracy of 1 second
    timeval tv;
    gettimeofday(&tv, NULL);
    signed64 queueTime = ((tv.tv_sec - j->submitTime) * 1000000) + tv.tv_usec;

    // "Wrote notification file"
    castor::dlf::Param param[] =
      {castor::dlf::Param("JobId", handler->jobId),
       castor::dlf::Param("Filename", handler->notifyFile),
       castor::dlf::Param("OpenFlags", handler->openFlags),
       castor::dlf::Param("Content", ipath.str()),
       castor::dlf::Param("Matches", handler->matches),
       castor::dlf::Param("Type", castor::ObjectsIdStrings[handler->requestType]),
       castor::dlf::Param("SvcClass", handler->svcClass),
       castor::dlf::Param("QueueTime",
                          queueTime > 0 ? queueTime * 0.000001 : 0),
       castor::dlf::Param(handler->subReqId)};
    castor::dlf::dlf_writep(handler->reqId, DLF_LVL_SYSTEM, 34, 9, param,
                            &handler->fileId);

    return 0;
  }


  //---------------------------------------------------------------------------
  // python_delete
  //---------------------------------------------------------------------------
  void python_delete(castor::scheduler::HandlerData *handler) {

    // Check parameters
    if (handler == NULL) {
      return;
    }

    // Remove notification file
    if (handler->notifyFile != "") {
      unlink(handler->notifyFile.c_str());
    }

    // Clean up the internal hash table and release memory
    castor::scheduler::hashTable.erase(handler->jobName.c_str());
    delete handler;
  }


  //---------------------------------------------------------------------------
  // python_rate
  //---------------------------------------------------------------------------
  void python_rate(void) {

    // This function is the start routine for the ratings thread of the plugin.
    // Its goal is to assign a rating to each filesystem in the shared memory
    // using python based policies.
    while (1) {

      // If we failed to initialise the python embedded interpreter try again.
      // If we fail sleep for PYINI_THROTTLING seconds so that we don't flood
      // the log file with error messages
      if (python == NULL) {
        try {
          python = new castor::scheduler::Python(policyFile);
        } catch (castor::exception::Exception e) {
          // Exception caught in initializing embedded Python interpreter"
          castor::dlf::Param params[] =
            {castor::dlf::Param("PolicyFile", policyFile),
             castor::dlf::Param("Type", sstrerror(e.code())),
             castor::dlf::Param("Message", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 82, 3, params);
          sleep(PYINI_THROTTLING);
          continue;
        }
      }

      // Loop over all diskservers
      u_signed64 processed = 0;
      for (castor::monitoring::ClusterStatus::iterator it =
             clusterStatus->begin(); it != clusterStatus->end(); it++) {

        // Loop over all filesystems
        for (castor::monitoring::DiskServerStatus::iterator it2 =
               it->second.begin(); it2 != it->second.end(); it2++) {

          // Do not recalculate the rating of this filesystem unless the
          // metrics related with it have been updated recently
          if (it2->second.lastRatingUpdate() >= it2->second.lastMetricsUpdate()) {
            continue;
          }
          it2->second.setLastRatingUpdate((u_signed64)time(NULL));

          // Setup the global variables in the python modules namespace
          python->pySet("Diskserver", it->first.c_str());
          python->pySet("Filesystem", it2->first.c_str());
          python->pySet("FreeSpace", it2->second.freeSpace());
          python->pySet("TotalSpace", it2->second.space());
          python->pySet("TotalNbFilesystems", (unsigned int)it->second.size());

          // The data rate and stream counts for this filesystem only
          python->pySet("ReadRate", it2->second.readRate() +
                        it2->second.deltaReadRate());
          python->pySet("WriteRate", it2->second.writeRate() +
                        it2->second.deltaWriteRate());
          python->pySet("NbReadStreams", it2->second.nbReadStreams() +
                        it2->second.deltaNbReadStreams());
          python->pySet("NbWriteStreams", it2->second.nbWriteStreams() +
                        it2->second.deltaNbWriteStreams());
          python->pySet("NbReadWriteStreams", it2->second.nbReadWriteStreams() +
                        it2->second.deltaNbReadWriteStreams());

          // The total values for data rates and stream counts across
          // the whole diskserver i.e. all filesystems
          python->pySet("TotalReadRate", it->second.readRate() +
                        it->second.deltaReadRate());
          python->pySet("TotalWriteRate", it->second.writeRate() +
                        it->second.deltaWriteRate());
          python->pySet("TotalNbReadStreams", it->second.nbReadStreams() +
                        it->second.deltaNbReadStreams());
          python->pySet("TotalNbWriteStreams", it->second.nbWriteStreams() +
                        it->second.deltaNbWriteStreams());
          python->pySet("TotalNbReadWriteStreams", it->second.nbReadWriteStreams() +
                        it->second.deltaNbReadWriteStreams());

          // Recaller and migrator streams. Note: there are no deltas to be
          // added here as migrations and recalls are scheduled through LSF
          python->pySet("NbRecallerStreams", it2->second.nbRecallerStreams());
          python->pySet("NbMigratorStreams", it2->second.nbMigratorStreams());
          python->pySet("TotalNbRecallerStreams", it->second.nbRecallerStreams());
          python->pySet("TotalNbMigratorStreams", it->second.nbMigratorStreams());

          // Update the ratings of the filesystem for all rating groups
          for (castor::monitoring::FileSystemStatus::iterator it3 =
                 it2->second.begin();
               it3 != it2->second.end(); it3++) {

            if (!it3->second.active()) {
              continue; // Rating group not active
            }

            try {
              processed++;
              python->pySet("OpenFlags", "r");
              it3->second.setReadRating(python->pyExecute(it3->first.c_str()));
              python->pySet("OpenFlags", "w");
              it3->second.setWriteRating(python->pyExecute(it3->first.c_str()));
              python->pySet("OpenFlags", "o");
              it3->second.setReadWriteRating(python->pyExecute(it3->first.c_str()));
            } catch (castor::exception::Exception e) {

              // We throttle error messages here as we have no delay/sleep
              // caused by the tight loop we run in
              if ((time(NULL) - it2->second.lastRatingError()) > PYERR_THROTTLING) {
                // "Error caught rating a filesystem"
                castor::dlf::Param params[] =
                  {castor::dlf::Param("DiskServer", it->first.c_str()),
                   castor::dlf::Param("Function", it3->first.c_str()),
                   castor::dlf::Param("Type", sstrerror(e.code())),
                   castor::dlf::Param("Error", e.getMessage().str())};
                castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 23, 4, params);
                it2->second.setLastRatingError((u_signed64)time(NULL));
              }
              // We set the ratings to a positive number which will cause this
              // filesystem to be excluded in the match phase
              it3->second.setReadRating(1.0);
              it3->second.setWriteRating(1.0);
              it3->second.setReadWriteRating(1.0);
            }
          }
        }
      }

      // If we didn't process any filesystems sleep for a bit
      if (processed == 0) {
        sleep(1);
      }
    }

    // We should never get here
    Cthread_exit(0);
  }

} // End of extern "C"
