/******************************************************************************
 *                      castor/stager/TapeCopy.hpp
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

#ifndef CASTOR_STAGER_TAPECOPY_HPP
#define CASTOR_STAGER_TAPECOPY_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class Stream;
    class Segment;
    class CastorFile;

    /**
     * class TapeCopy
     * One copy of a given file on a tape
     */
    class TapeCopy : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      TapeCopy() throw();

      /**
       * Empty Destructor
       */
      virtual ~TapeCopy() throw();

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

      /*********************************/
      /* End of IObject abstract class */
      /*********************************/
      /**
       * Add a Stream* object to the m_streamVector list
       */
      void addStream(Stream* add_object) {
        m_streamVector.push_back(add_object);
      }

      /**
       * Remove a Stream* object from m_streamVector
       */
      void removeStream(Stream* remove_object) {
        for (unsigned int i = 0; i < m_streamVector.size(); i++) {
          Stream* item = m_streamVector[i];
          if (item == remove_object) {
            std::vector<Stream*>::iterator it = m_streamVector.begin() + i;
            m_streamVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of Stream* objects held by m_streamVector
       * @return list of Stream* objects held by m_streamVector
       */
      std::vector<Stream*>& stream() {
        return m_streamVector;
      }

      /**
       * Add a Segment* object to the m_segmentsVector list
       */
      void addSegments(Segment* add_object) {
        m_segmentsVector.push_back(add_object);
      }

      /**
       * Remove a Segment* object from m_segmentsVector
       */
      void removeSegments(Segment* remove_object) {
        for (unsigned int i = 0; i < m_segmentsVector.size(); i++) {
          Segment* item = m_segmentsVector[i];
          if (item == remove_object) {
            std::vector<Segment*>::iterator it = m_segmentsVector.begin() + i;
            m_segmentsVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of Segment* objects held by m_segmentsVector
       * @return list of Segment* objects held by m_segmentsVector
       */
      std::vector<Segment*>& segments() {
        return m_segmentsVector;
      }

      /**
       * Get the value of m_castorFile
       * @return the value of m_castorFile
       */
      CastorFile* castorFile() const {
        return m_castorFile;
      }

      /**
       * Set the value of m_castorFile
       * @param new_var the new value of m_castorFile
       */
      void setCastorFile(CastorFile* new_var) {
        m_castorFile = new_var;
      }

      /**
       * Get the value of m_status
       * @return the value of m_status
       */
      TapeCopyStatusCodes status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * @param new_var the new value of m_status
       */
      void setStatus(TapeCopyStatusCodes new_var) {
        m_status = new_var;
      }

    private:

    private:

      /// The id of this object
      u_signed64 m_id;

      std::vector<Stream*> m_streamVector;

      std::vector<Segment*> m_segmentsVector;

      CastorFile* m_castorFile;

      TapeCopyStatusCodes m_status;

    }; // end of class TapeCopy

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_TAPECOPY_HPP
