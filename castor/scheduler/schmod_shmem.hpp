/******************************************************************************
 *                      schmod_shmem.hpp
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
 * @(#)$RCSfile: schmod_shmem.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/02/22 08:57:52 $ $Author: waldron $
 *
 * Header file for the Castor LSF External Plugin - Phase 1 (Shared Memory)
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef SCHEDULER_PLUGIN_SHMEM_HPP
#define SCHEDULER_PLUGIN_SHMEM_HPP 1

// Include files
#include "castor/scheduler/HandlerData.hpp"
#include "castor/scheduler/Constants.hpp"


/**
 * C functions required by the LSF Framework
 */
extern "C" {

  /**
   * Initialization of the module
   * @param param unused
   * @return 0 if successful or -1 with errno set appropriately
   */
  int sched_init(void *param);

  /**
   * Returns the plugin version
   */
  int sched_version(void *param);

  /**
   * Hook called when a new job enters LSF.
   * @param resreq pointer to the internal resource data structure of the job
   * @return 0 if successful or -1 with errno set appropriately
   */
  int shmem_new(void *resreq);

  /**
   * Filter to remove hosts from the candidate group list which do not meet
   * the requirements of the job
   * @param handler the handler specific data associated with the job
   * @param candGroupList the list of hosts that could run the job
   * @param reasonTB the table of pending reasons
   * @return 0 if successful or -1 with errno set appropriately
   */
  int shmem_match(castor::scheduler::HandlerData *handler,
		  void *candGroupList,
		  void *reasonTb);

  /**
   * Deletes the handler specific data associated with the job
   * @param handler pointer to the HandlerData
   */
  void shmem_delete(castor::scheduler::HandlerData *handler);

} // End of extern "C"

#endif // SCHEDULER_PLUGIN_SHMEM_HPP
