/******************************************************************************
 *                      castor/stager/FileClass.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_FILECLASS_HPP
#define CASTOR_STAGER_FILECLASS_HPP

// Include Files
#include "castor/IObject.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    /**
     * class FileClass
     * The FileClass of a file defines several attributes like the number of copies of
     * the file or its tapepool
     */
    class FileClass : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      FileClass() throw();

      /**
       * Empty Destructor
       */
      virtual ~FileClass() throw();

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
       * Get the value of m_name
       * The name of the FileClass
       * @return the value of m_name
       */
      std::string name() const {
        return m_name;
      }

      /**
       * Set the value of m_name
       * The name of the FileClass
       * @param new_var the new value of m_name
       */
      void setName(std::string new_var) {
        m_name = new_var;
      }

      /**
       * Get the value of m_minFileSize
       * The minimum size of a file in this FileClass
       * @return the value of m_minFileSize
       */
      u_signed64 minFileSize() const {
        return m_minFileSize;
      }

      /**
       * Set the value of m_minFileSize
       * The minimum size of a file in this FileClass
       * @param new_var the new value of m_minFileSize
       */
      void setMinFileSize(u_signed64 new_var) {
        m_minFileSize = new_var;
      }

      /**
       * Get the value of m_maxFileSize
       * The maximum size of a file in this FileClass
       * @return the value of m_maxFileSize
       */
      u_signed64 maxFileSize() const {
        return m_maxFileSize;
      }

      /**
       * Set the value of m_maxFileSize
       * The maximum size of a file in this FileClass
       * @param new_var the new value of m_maxFileSize
       */
      void setMaxFileSize(u_signed64 new_var) {
        m_maxFileSize = new_var;
      }

      /**
       * Get the value of m_nbCopies
       * The number of copies on tape for a file of this FileClass
       * @return the value of m_nbCopies
       */
      unsigned int nbCopies() const {
        return m_nbCopies;
      }

      /**
       * Set the value of m_nbCopies
       * The number of copies on tape for a file of this FileClass
       * @param new_var the new value of m_nbCopies
       */
      void setNbCopies(unsigned int new_var) {
        m_nbCopies = new_var;
      }

    private:

    private:

      /// The name of the FileClass
      std::string m_name;

      /// The minimum size of a file in this FileClass
      u_signed64 m_minFileSize;

      /// The maximum size of a file in this FileClass
      u_signed64 m_maxFileSize;

      /// The number of copies on tape for a file of this FileClass
      unsigned int m_nbCopies;

      /// The id of this object
      u_signed64 m_id;

    }; // end of class FileClass

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_FILECLASS_HPP
