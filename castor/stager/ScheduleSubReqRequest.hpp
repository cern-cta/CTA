/******************************************************************************
 *                      castor/stager/ScheduleSubReqRequest.hpp
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
 * @(#)$RCSfile: ScheduleSubReqRequest.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/24 11:52:24 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_SCHEDULESUBREQREQUEST_HPP
#define CASTOR_STAGER_SCHEDULESUBREQREQUEST_HPP

// Include Files
#include "castor/stager/Request.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;
  class IObject;

  namespace stager {

    /**
     * class ScheduleSubReqRequest
     * Internal request for scheduling of a subrequest. This request is there to avoid
     * the jobs on the diskservers to handle a connection to the database. 
     */
    class ScheduleSubReqRequest : public virtual Request {

    public:

      /**
       * Empty Constructor
       */
      ScheduleSubReqRequest() throw();

      /**
       * Empty Destructor
       */
      virtual ~ScheduleSubReqRequest() throw();

      /**
       * Outputs this object in a human readable format
       * @param stream The stream where to print this object
       * @param indent The indentation to use
       * @param alreadyPrinted The set of objects already printed.
       * This is to avoid looping when printing circular dependencies
       */
      virtual void print(std::ostream& stream,
                         std::string indent,
                         castor::ObjectSet& alreadyPrinted) const;

      /**
       * Outputs this object in a human readable format
       */
      virtual void print() const;

      /**
       * Gets the type of this kind of objects
       */
      static int TYPE();

      /********************************************/
      /* Implementation of IObject abstract class */
      /********************************************/
      /**
       * Sets the id of the object
       * @param id The new id
       */
      virtual void setId(u_signed64 id);

      /**
       * gets the id of the object
       */
      virtual u_signed64 id() const;

      /**
       * Gets the type of the object
       */
      virtual int type() const;

      /**
       * virtual method to clone any object
       */
      virtual castor::IObject* clone();

      /*********************************/
      /* End of IObject abstract class */
      /*********************************/
      /**
       * Get the value of m_subreqId
       * The id of the subRequest that should be scheduled
       * @return the value of m_subreqId
       */
      u_signed64 subreqId() const {
        return m_subreqId;
      }

      /**
       * Set the value of m_subreqId
       * The id of the subRequest that should be scheduled
       * @param new_var the new value of m_subreqId
       */
      void setSubreqId(u_signed64 new_var) {
        m_subreqId = new_var;
      }

      /**
       * Get the value of m_diskServer
       * The name of the diskserver on which the selected filesystem for the given
       * SubRequest resides
       * @return the value of m_diskServer
       */
      std::string diskServer() const {
        return m_diskServer;
      }

      /**
       * Set the value of m_diskServer
       * The name of the diskserver on which the selected filesystem for the given
       * SubRequest resides
       * @param new_var the new value of m_diskServer
       */
      void setDiskServer(std::string new_var) {
        m_diskServer = new_var;
      }

      /**
       * Get the value of m_fileSystem
       * The mount point of the selected filesystem for the given SubRequest
       * @return the value of m_fileSystem
       */
      std::string fileSystem() const {
        return m_fileSystem;
      }

      /**
       * Set the value of m_fileSystem
       * The mount point of the selected filesystem for the given SubRequest
       * @param new_var the new value of m_fileSystem
       */
      void setFileSystem(std::string new_var) {
        m_fileSystem = new_var;
      }

    private:

    private:

      /// The id of the subRequest that should be scheduled
      u_signed64 m_subreqId;

      /// The name of the diskserver on which the selected filesystem for the given SubRequest resides
      std::string m_diskServer;

      /// The mount point of the selected filesystem for the given SubRequest
      std::string m_fileSystem;

      /// The id of this object
      u_signed64 m_id;

    }; // end of class ScheduleSubReqRequest

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_SCHEDULESUBREQREQUEST_HPP
