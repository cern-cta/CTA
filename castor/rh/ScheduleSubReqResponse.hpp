/******************************************************************************
 *                      castor/rh/ScheduleSubReqResponse.hpp
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
 * @(#)$RCSfile: ScheduleSubReqResponse.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/24 11:52:24 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_RH_SCHEDULESUBREQRESPONSE_HPP
#define CASTOR_RH_SCHEDULESUBREQRESPONSE_HPP

// Include Files
#include "castor/rh/Response.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;
  class IObject;

  // Forward declarations
  namespace stager {

    // Forward declarations
    class DiskCopy;

  }; // end of namespace stager

  namespace rh {

    /**
     * class ScheduleSubReqResponse
     * 
     */
    class ScheduleSubReqResponse : public virtual Response {

    public:

      /**
       * Empty Constructor
       */
      ScheduleSubReqResponse() throw();

      /**
       * Empty Destructor
       */
      virtual ~ScheduleSubReqResponse() throw();

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
       * Get the value of m_diskCopy
       * @return the value of m_diskCopy
       */
      castor::stager::DiskCopy* diskCopy() const {
        return m_diskCopy;
      }

      /**
       * Set the value of m_diskCopy
       * @param new_var the new value of m_diskCopy
       */
      void setDiskCopy(castor::stager::DiskCopy* new_var) {
        m_diskCopy = new_var;
      }

      /**
       * Add a castor::stager::DiskCopy* object to the m_sourcesVector list
       */
      void addSources(castor::stager::DiskCopy* add_object) {
        m_sourcesVector.push_back(add_object);
      }

      /**
       * Remove a castor::stager::DiskCopy* object from m_sourcesVector
       */
      void removeSources(castor::stager::DiskCopy* remove_object) {
        for (unsigned int i = 0; i < m_sourcesVector.size(); i++) {
          castor::stager::DiskCopy* item = m_sourcesVector[i];
          if (item == remove_object) {
            std::vector<castor::stager::DiskCopy*>::iterator it = m_sourcesVector.begin() + i;
            m_sourcesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of castor::stager::DiskCopy* objects held by m_sourcesVector
       * @return list of castor::stager::DiskCopy* objects held by m_sourcesVector
       */
      std::vector<castor::stager::DiskCopy*>& sources() {
        return m_sourcesVector;
      }

    private:

    private:

      /// The id of this object
      u_signed64 m_id;

      castor::stager::DiskCopy* m_diskCopy;

      std::vector<castor::stager::DiskCopy*> m_sourcesVector;

    }; // end of class ScheduleSubReqResponse

  }; // end of namespace rh

}; // end of namespace castor

#endif // CASTOR_RH_SCHEDULESUBREQRESPONSE_HPP
