/******************************************************************************
 *                      castor/stager/StageInRequest.hpp
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
 * @(#)$RCSfile: StageInRequest.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:25 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_STAGEINREQUEST_HPP
#define CASTOR_STAGER_STAGEINREQUEST_HPP

// Include Files
#include "castor/stager/Request.hpp"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    /**
     * class StageInRequest
     * A stagein request 
     */
    class StageInRequest : public virtual Request {

    public:

      /**
       * Empty Constructor
       */
      StageInRequest() throw();

      /**
       * Empty Destructor
       */
      virtual ~StageInRequest() throw();

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
       * Get the value of m_openflags
       * If it contains O_RDWR or O_WRONLY the file is considered to be opened by an
       * application that want to modify it.
       * @return the value of m_openflags
       */
      int openflags() const {
        return m_openflags;
      }

      /**
       * Set the value of m_openflags
       * If it contains O_RDWR or O_WRONLY the file is considered to be opened by an
       * application that want to modify it.
       * @param new_var the new value of m_openflags
       */
      void setOpenflags(int new_var) {
        m_openflags = new_var;
      }

    private:

    private:

      /// If it contains O_RDWR or O_WRONLY the file is considered to be opened by an application that want to modify it.
      int m_openflags;

      /// The id of this object
      unsigned long m_id;

    }; // end of class StageInRequest

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_STAGEINREQUEST_HPP
