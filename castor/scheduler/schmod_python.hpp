/******************************************************************************
 *                      schmod_python.hpp
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
 * @(#)$RCSfile: schmod_python.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/03/03 13:26:50 $ $Author: waldron $
 *
 * Header file for the Castor LSF External Plugin - Phase 2 (Python)
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef SCHEDULER_PLUGIN_PYTHON_HPP
#define SCHEDULER_PLUGIN_PYTHON_HPP 1

// Include files
#include "castor/scheduler/Python.hpp"
#include "castor/scheduler/HandlerData.hpp"
#include "castor/scheduler/Constants.hpp"
#include "getconfent.h"
#include "u64subr.h"
#include <map>


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
   * Initialize the coefficients to be used when updating the delta values
   * in the shared memory after filesystem selection.
   */
  void python_init_coeffs();

  /**
   * Hook called when a new job enters LSF. 
   * @param resreq pointer to the internal resource data structure of the job
   * @return 0 if successful or -1 with errno set appropriately
   */
  int python_new(void *resreq);

  /**
   * Filter to remove hosts from the candidate group list which do not meet 
   * the requirements of the job
   * @param handler the handler specific data associated with the job
   * @param candGroupList the list of hosts that could run the job
   * @param reasonTB the table of pending reasons
   * @return 0 if successful or -1 with errno set appropriately
   */
  int python_match(castor::scheduler::HandlerData *handler,
		   void *candGroupList,
		   void *reasonTb);
  
  /**
   * Function used to update the delta values for a given diskserver and
   * filesystem in the shared memory
   * @param diskServer the selected diskserver
   * @param fileSystem the selected filesystem
   * @param openFlags the direction of the transfer
   * @param fileSize the size of the castor file in bytes
   */
  void python_update_deltas(std::string diskServer,
			    std::string fileSystem,
			    std::string openFlags,
			    u_signed64 fileSize);

  /**
   * Hook called when a new job is scheduled
   */
  int python_notify(void *info,
		    void *job,
		    void *alloc,
		    void *allocLimitList,
		    int  notifyFlag);

  /**
   * Deletes the handler specific data associated with the job
   * @param handler pointer to the HandlerData
   */
  void python_delete(castor::scheduler::HandlerData *handler);

  /**
   * The python_rate function is used to calculate the rating of filesystems
   * using Pyhon in an asychronous fashion. The function is executed in its
   * out thread so that the processing overhead does not impact the general
   * performance of LSF.
   */
  void python_rate(void);
  
} // End of extern "C"


/**
 * C++ code for internal use
 */
namespace castor {

  namespace scheduler {

    /**
     * A map storing handler related information to be recalled
     * at various phases of a jobs lifecycle
     */
    std::map<const char *, castor::scheduler::HandlerData *> hashTable;

    /**
     * Gets a integer value from castor.conf
     * @param var
     * @param name the name of the option to lookup in castor.conf
     */
    void getIntCoeff(int &var, const char *name) {
      char *value = getconfent("SchedCoeffs", name, 0);
      if (value != NULL) {
	var = atoi(value);
      }
    }

    /**
     * Gets a 64bit integer value from castor.conf
     * @param var
     * @param name the name of the option to lookup in castor.conf
     */
    void getU64Coeff(u_signed64 &var, const char *name) {
      char *value = getconfent("SchedCoeffs", name, 0);
      if (value != NULL) {
	var = strutou64(value);
      }
    }
    
  } // End of namespace scheduler

} // End of namespace castor

#endif // SCHEDULER_PLUGIN_PYTHON_HPP
