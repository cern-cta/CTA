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
 * @(#)$RCSfile: ScheduleSubReqResponse.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/30 08:57:50 $ $Author: sponcec3 $
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
    class DiskCopyForRecall;
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
       * Get the value of m_id
       * The id of this object
       * @return the value of m_id
       */
      u_signed64 id() const {
        return m_id;
      }

      /**
       * Set the value of m_id
       * The id of this object
       * @param new_var the new value of m_id
       */
      void setId(u_signed64 new_var) {
        m_id = new_var;
      }

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
       * Add a castor::stager::DiskCopyForRecall* object to the m_sourcesVector list
       */
      void addSources(castor::stager::DiskCopyForRecall* add_object) {
        m_sourcesVector.push_back(add_object);
      }

      /**
       * Remove a castor::stager::DiskCopyForRecall* object from m_sourcesVector
       */
      void removeSources(castor::stager::DiskCopyForRecall* remove_object) {
        for (unsigned int i = 0; i < m_sourcesVector.size(); i++) {
          castor::stager::DiskCopyForRecall* item = m_sourcesVector[i];
          if (item == remove_object) {
            std::vector<castor::stager::DiskCopyForRecall*>::iterator it = m_sourcesVector.begin() + i;
            m_sourcesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of castor::stager::DiskCopyForRecall* objects held by
       * m_sourcesVector
       * @return list of castor::stager::DiskCopyForRecall* objects held by
       * m_sourcesVector
       */
      std::vector<castor::stager::DiskCopyForRecall*>& sources() {
        return m_sourcesVector;
      }

    private:

      /// The id of this object
      u_signed64 m_id;

      castor::stager::DiskCopy* m_diskCopy;

      std::vector<castor::stager::DiskCopyForRecall*> m_sourcesVector;

    }; // end of class ScheduleSubReqResponse

  }; // end of namespace rh

}; // end of namespace castor

#endif // CASTOR_RH_SCHEDULESUBREQRESPONSE_HPP
