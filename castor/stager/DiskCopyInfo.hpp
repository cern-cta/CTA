/******************************************************************************
 *                      castor/stager/DiskCopyInfo.hpp
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

#ifndef CASTOR_STAGER_DISKCOPYINFO_HPP
#define CASTOR_STAGER_DISKCOPYINFO_HPP

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
     * class DiskCopyInfo
     * 
     */
    class DiskCopyInfo : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      DiskCopyInfo() throw();

      /**
       * Empty Destructor
       */
      virtual ~DiskCopyInfo() throw();

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
       * Get the value of m_DiskCopyPath
       * Path of the DiskCopy described
       * @return the value of m_DiskCopyPath
       */
      std::string DiskCopyPath() const {
        return m_DiskCopyPath;
      }

      /**
       * Set the value of m_DiskCopyPath
       * Path of the DiskCopy described
       * @param new_var the new value of m_DiskCopyPath
       */
      void setDiskCopyPath(std::string new_var) {
        m_DiskCopyPath = new_var;
      }

      /**
       * Get the value of m_size
       * Size of the underlying CastorFile
       * @return the value of m_size
       */
      u_signed64 size() const {
        return m_size;
      }

      /**
       * Set the value of m_size
       * Size of the underlying CastorFile
       * @param new_var the new value of m_size
       */
      void setSize(u_signed64 new_var) {
        m_size = new_var;
      }

      /**
       * Get the value of m_diskCopyStatus
       * Status of the DiskCopy described
       * @return the value of m_diskCopyStatus
       */
      int diskCopyStatus() const {
        return m_diskCopyStatus;
      }

      /**
       * Set the value of m_diskCopyStatus
       * Status of the DiskCopy described
       * @param new_var the new value of m_diskCopyStatus
       */
      void setDiskCopyStatus(int new_var) {
        m_diskCopyStatus = new_var;
      }

      /**
       * Get the value of m_tapeCopyStatus
       * Status of the tapeCopy(ies?) associated to the underlying CastorFile
       * @return the value of m_tapeCopyStatus
       */
      int tapeCopyStatus() const {
        return m_tapeCopyStatus;
      }

      /**
       * Set the value of m_tapeCopyStatus
       * Status of the tapeCopy(ies?) associated to the underlying CastorFile
       * @param new_var the new value of m_tapeCopyStatus
       */
      void setTapeCopyStatus(int new_var) {
        m_tapeCopyStatus = new_var;
      }

      /**
       * Get the value of m_segmentStatus
       * Status of the segment(s?) associated to the underlying CastorFile
       * @return the value of m_segmentStatus
       */
      int segmentStatus() const {
        return m_segmentStatus;
      }

      /**
       * Set the value of m_segmentStatus
       * Status of the segment(s?) associated to the underlying CastorFile
       * @param new_var the new value of m_segmentStatus
       */
      void setSegmentStatus(int new_var) {
        m_segmentStatus = new_var;
      }

      /**
       * Get the value of m_DiskCopyId
       * Id of the DiskCopy Described
       * @return the value of m_DiskCopyId
       */
      u_signed64 DiskCopyId() const {
        return m_DiskCopyId;
      }

      /**
       * Set the value of m_DiskCopyId
       * Id of the DiskCopy Described
       * @param new_var the new value of m_DiskCopyId
       */
      void setDiskCopyId(u_signed64 new_var) {
        m_DiskCopyId = new_var;
      }

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

    private:

      /// Path of the DiskCopy described
      std::string m_DiskCopyPath;

      /// Size of the underlying CastorFile
      u_signed64 m_size;

      /// Status of the DiskCopy described
      int m_diskCopyStatus;

      /// Status of the tapeCopy(ies?) associated to the underlying CastorFile
      int m_tapeCopyStatus;

      /// Status of the segment(s?) associated to the underlying CastorFile
      int m_segmentStatus;

      /// Id of the DiskCopy Described
      u_signed64 m_DiskCopyId;

      /// The id of this object
      u_signed64 m_id;

    }; // end of class DiskCopyInfo

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_DISKCOPYINFO_HPP
