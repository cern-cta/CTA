/******************************************************************************
 *                      castor/stager/Stream.hpp
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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_STREAM_HPP
#define CASTOR_STAGER_STREAM_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    /**
     * class Stream
     * A logical stream for the writing of DiskCopies on Tapes
     */
    class Stream : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      Stream() throw();

      /**
       * Empty Destructor
       */
      virtual ~Stream() throw();

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
      virtual void setId(unsigned long id);

      /**
       * gets the id of the object
       */
      virtual unsigned long id() const;

      /**
       * Gets the type of the object
       */
      virtual int type() const;

      /*********************************/
      /* End of IObject abstract class */
      /*********************************/
      /**
       * Get the value of m_initialSizeToTransfer
       * Initial data volume to be migrated (needed by vmgr_gettape())
       * @return the value of m_initialSizeToTransfer
       */
      u_signed64 initialSizeToTransfer() const {
        return m_initialSizeToTransfer;
      }

      /**
       * Set the value of m_initialSizeToTransfer
       * Initial data volume to be migrated (needed by vmgr_gettape())
       * @param new_var the new value of m_initialSizeToTransfer
       */
      void setInitialSizeToTransfer(u_signed64 new_var) {
        m_initialSizeToTransfer = new_var;
      }

      /**
       * Get the value of m_status
       * @return the value of m_status
       */
      StreamStatusCodes status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * @param new_var the new value of m_status
       */
      void setStatus(StreamStatusCodes new_var) {
        m_status = new_var;
      }

    private:

    private:

      /// Initial data volume to be migrated (needed by vmgr_gettape())
      u_signed64 m_initialSizeToTransfer;

      /// The id of this object
      unsigned long m_id;

      StreamStatusCodes m_status;

    }; // end of class Stream

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_STREAM_HPP
