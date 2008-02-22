/******************************************************************************
 *                      FileSystemRating.hpp
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
 * @(#)$RCSfile: FileSystemRating.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/02/22 08:48:11 $ $Author: waldron $
 *
 * Describes the rating given to the filesystem by the scheduler
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef MONITORING_FILESYSTEMRATING_HPP
#define MONITORING_FILESYSTEMRATING_HPP 1

// Include files
#include "castor/monitoring/SharedMemoryString.hpp"
#include <map>
#include <iostream>
#include "osdep.h"


namespace castor {

  namespace monitoring {

    /*
     * Describes the rating given to the filesystem by the scheduler
     */
    class FileSystemRating {
      
    public:

      /**
       * Constructor
       */
      FileSystemRating();     

      /**
       * print method
       */
      void print(std::ostream& out,
		 const std::string& indentation = "",
		 const bool showAll = true) const
        throw();

    public:

      /// Accessor to active
      void setActive(bool new_var) {
	m_active = new_var;
      }

      /// Accessor to active
      bool active() const {
	return m_active;
      }

      /// Accessor to readRating
      void setReadRating(float new_var) {
	m_readRating = new_var;
      }

      /// Accessor to readRating
      float readRating() const {
	return m_readRating;
      }

      /// Accessor to writeRating
      void setWriteRating(float new_var) {
	m_writeRating = new_var;
      }

      /// Accessor to writeRating
      float writeRating() const {
	return m_writeRating;
      }

      /// Accessor to readWriteRating
      void setReadWriteRating(float new_var) {
	m_readWriteRating = new_var;
      }

      /// Accessor to readWriteRating
      float readWriteRating() const {
	return m_readWriteRating;
      }

    public:

      /**
       * Flag to indicate whether this diskpool rating is valid. As we don't 
       * delete from shared memory it is necessary to have a way to indicate 
       * that a filesystem no long belongs to a certain service class
       */
      bool m_active;

      /// The rating given to the filesystem for read requests
      float m_readRating;

      /// The rating given to the filesystem for write requests
      float m_writeRating;

      /// The rating given to the filesystem for read-write requests
      float m_readWriteRating;

   }; /* end of class FileSystemRating */

  } /* end of namespace monitoring */

} /* end of namespace castor */

#endif // MONITORING_FILESYSTEMRATING_HPP
