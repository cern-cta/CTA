/******************************************************************************
 *                      FileSystemStatus.hpp
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
 * Describes the status of one file system
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RMMASTER_FILESYSTEMSTATUS_HPP
#define RMMASTER_FILESYSTEMSTATUS_HPP 1

#include <iostream>
#include "osdep.h"
#include "castor/sharedMemory/string"

namespace castor {

  namespace rmmaster {

    /*
     * Describes the status of one machine
     */
    class FileSystemStatus {

    public:

      /*
       * Constructor
       */
      FileSystemStatus(u_signed64 id = 0);

      /**
       * print method
       */
      void print(std::ostream& out) const throw();

    public:

      /// Accessor to id
      u_signed64 id() const { return m_id; }

      /// Accessor to id
      void setId(u_signed64 id) { m_id = id; }

      /// Accessor to weigth
      float weight() const { return m_weight; }

      /// Accessor to weigth
      void setWeight(float weight) { m_weight = weight; }

      /// Accessor to deltaWeight
      float deltaWeight() const { return m_deltaWeight; }

      /// Accessor to deltaWeight
      void setDeltaWeight(float deltaWeight) { m_deltaWeight = deltaWeight; }

      /// Accessor to deviation
      float deviation() const { return m_deviation; }

      /// Accessor to deviation
      void setDeviation(float deviation) { m_deviation = deviation; }

      /// Accessor to available space
      u_signed64 availableSpace() const { return m_availableSpace; }

      /// Accessor to available space
      void setAvailableSpace(u_signed64 availableSpace) { m_availableSpace = availableSpace; }

    private:

      /// the filesystem id
      u_signed64 m_id;

      /// the fileSystem weigth
      float m_weight;

      /// the fileSystem deltaWeight
      float m_deltaWeight;

      /// the fileSystem deviation
      float m_deviation;

      /// available space
      u_signed64 m_availableSpace;

    }; // end FileSystemStatus

  } // end rmmaster

} // end castor

#endif // RMMASTER_FILESYSTEMSTATUS_HPP
