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

#ifndef MONITORING_FILESYSTEMSTATUS_HPP
#define MONITORING_FILESYSTEMSTATUS_HPP 1

#include <iostream>
#include "osdep.h"
#include "castor/stager/FileSystemStatusCodes.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"

namespace castor {

  namespace monitoring {

    /*
     * Describes the status of one machine
     */
    class FileSystemStatus {

    public:

      /*
       * Constructor
       */
      FileSystemStatus();

      /**
       * print method
       */
      void print(std::ostream& out,
                 const std::string& indentation = "") const
        throw();

    public:

      /// Accessor to space
      u_signed64 space() const { return m_space; }

      /// Accessor to space
      void setSpace(u_signed64 space) { m_space = space; }

      /// Accessor to minFreeSpace
      float minFreeSpace() const {
        return m_minFreeSpace;
      }

      /// Accessor to minFreeSpace
      void setMinFreeSpace(float new_var) {
        m_minFreeSpace = new_var;
      }

      /// Accessor to maxFreeSpace
      float maxFreeSpace() const {
        return m_maxFreeSpace;
      }

      /// Accessor to maxFreeSpace
      void setMaxFreeSpace(float new_var) {
        m_maxFreeSpace = new_var;
      }

      /// Accessor to minAllowedFreeSpace
      float minAllowedFreeSpace() const {
        return m_minAllowedFreeSpace;
      }

      /// Accessor to minAllowedFreeSpace
      void setMinAllowedFreeSpace(float new_var) {
        m_minAllowedFreeSpace = new_var;
      }

      /// Accessor to status
      castor::stager::FileSystemStatusCodes status() const {
        return m_status;
      }

      /// Accessor to status
      void setStatus(castor::stager::FileSystemStatusCodes status) {
        m_status = status;
      }

      /// Accessor to adminStatus
      castor::monitoring::AdminStatusCodes adminStatus() const {
        return m_adminStatus;
      }

      /// Accessor to adminStatus
      void setAdminStatus(castor::monitoring::AdminStatusCodes adminStatus) {
        m_adminStatus = adminStatus;
      }

      /// Accessor to readRate
      u_signed64 readRate() const { return m_readRate; }

      /// Accessor to readRate
      void setReadRate(u_signed64 readRate) { m_readRate = readRate; }

      /// Accessor to deltaReadRate
      signed64 deltaReadRate() const { return m_deltaReadRate; }

      /// Accessor to deltaReadRate
      void setDeltaReadRate(signed64 deltaReadRate) {
	m_deltaReadRate = deltaReadRate;
      }

      /// Accessor to writeRate
      u_signed64 writeRate() const { return m_writeRate; }

      /// Accessor to writeRate
      void setWriteRate(u_signed64 writeRate) { m_writeRate = writeRate; }

      /// Accessor to deltaWriteRate
      signed64 deltaWriteRate() const { return m_deltaWriteRate; }

      /// Accessor to deltaWriteRate
      void setDeltaWriteRate(signed64 deltaWriteRate) {
	m_deltaWriteRate = deltaWriteRate;
      }

      /// Accessor to nbReadStreams
      unsigned int nbReadStreams() const { return m_nbReadStreams; }

      /// Accessor to nbReadStreams
      void setNbReadStreams(unsigned int nbReadStreams) { m_nbReadStreams = nbReadStreams; }

      /// Accessor to deltaNbReadStreams
      int deltaNbReadStreams() const { return m_deltaNbReadStreams; }

      /// Accessor to deltaNbReadStreams
      void setDeltaNbReadStreams(int deltaNbReadStreams) {
	m_deltaNbReadStreams = deltaNbReadStreams;
      }

      /// Accessor to nbWriteStreams
      unsigned int nbWriteStreams() const { return m_nbWriteStreams; }

      /// Accessor to nbWriteStreams
      void setNbWriteStreams(unsigned int nbWriteStreams) { m_nbWriteStreams = nbWriteStreams; }

      /// Accessor to deltaNbWriteStreams
      int deltaNbWriteStreams() const { return m_deltaNbWriteStreams; }

      /// Accessor to deltaNbWriteStreams
      void setDeltaNbWriteStreams(int deltaNbWriteStreams) {
	m_deltaNbWriteStreams = deltaNbWriteStreams;
      }

      /// Accessor to nbReadWriteStreams
      unsigned int nbReadWriteStreams() const { return m_nbReadWriteStreams; }

      /// Accessor to nbReadWriteStreams
      void setNbReadWriteStreams(unsigned int nbReadWriteStreams) {
	m_nbReadWriteStreams = nbReadWriteStreams;
      }

      /// Accessor to deltaNbReadWriteStreams
      int deltaNbReadWriteStreams() const { return m_deltaNbReadWriteStreams; }

      /// Accessor to deltaNbReadWriteStreams
      void setDeltaNbReadWriteStreams(int deltaNbReadWriteStreams)
      { m_deltaNbReadWriteStreams = deltaNbReadWriteStreams; }

      /// Accessor to nbMigratorStreams
      unsigned int nbMigratorStreams() const { return m_nbMigratorStreams; }

      /// Accessor to nbMigratorStreams
      void setNbMigratorStreams(unsigned int nbMigratorStreams) { m_nbMigratorStreams = nbMigratorStreams; }

      /// Accessor to deltaNbMigratorStreams
      int deltaNbMigratorStreams() const { return m_deltaNbMigratorStreams; }

      /// Accessor to deltaNbMigratorStreams
      void setDeltaNbMigratorStreams(int deltaNbMigratorStreams) {
	m_deltaNbMigratorStreams = deltaNbMigratorStreams;
      }

      /// Accessor to nbRecallerStreams
      unsigned int nbRecallerStreams() const { return m_nbRecallerStreams; }

      /// Accessor to nbRecallerStreams
      void setNbRecallerStreams(unsigned int nbRecallerStreams) { m_nbRecallerStreams = nbRecallerStreams; }

      /// Accessor to deltaNbRecallerStreams
      int deltaNbRecallerStreams() const { return m_deltaNbRecallerStreams; }

      /// Accessor to deltaNbRecallerStreams
      void setDeltaNbRecallerStreams(int deltaNbRecallerStreams) {
	m_deltaNbRecallerStreams = deltaNbRecallerStreams;
      }

      /// Accessor to free space
      u_signed64 freeSpace() const { return m_freeSpace; }

      /// Accessor to free space
      void setFreeSpace(u_signed64 freeSpace) { m_freeSpace = freeSpace; }

      /// Accessor to deltaFreeSpace
      signed64 deltaFreeSpace() const { return m_deltaFreeSpace; }

      /// Accessor to deltaFreeSpace
      void setDeltaFreeSpace(signed64 deltaFreeSpace) {
	m_deltaFreeSpace = deltaFreeSpace;
      }

      /// Accessor to lastStateupdate
      void setLastStateUpdate (u_signed64 lastStateUpdate) {
        m_lastStateUpdate = lastStateUpdate;
      }

      /// Accessor to lastStateupdate
      u_signed64 lastStateUpdate () { return m_lastStateUpdate; }

      /// Accessor to lastMetricsupdate
      void setLastMetricsUpdate (u_signed64 lastMetricsUpdate) {
        m_lastMetricsUpdate = lastMetricsUpdate;
      }

      /// Accessor to lastMetricsupdate
      u_signed64 lastMetricsUpdate () { return m_lastMetricsUpdate; }

    private:

      /// total space
      u_signed64 m_space;

      /// The fraction of free space under which the garbage collector will run. This number must be < 1
      float m_minFreeSpace;

      /// The fraction of free space that the garbage collector is targeting when liberating space. This number must be < 1
      float m_maxFreeSpace;

      /// The fraction of free space under which we stop scheduling write jobs on the filesystem. This number must be < 1
      float m_minAllowedFreeSpace;

      /// status
      castor::stager::FileSystemStatusCodes m_status;

      /// admin status
      castor::monitoring::AdminStatusCodes m_adminStatus;

      /// The number of bytes read per second
      u_signed64 m_readRate;

      /// Delta on the number of bytes read per second
      signed64 m_deltaReadRate;

      /// The number of bytes written per second
      u_signed64 m_writeRate;

      /// Delta on the number of bytes written per second
      signed64 m_deltaWriteRate;

      /// The number of read streams
      unsigned int m_nbReadStreams;

      /// Delta on the number of read streams
      int m_deltaNbReadStreams;

      /// The number of write streams
      unsigned int m_nbWriteStreams;

      /// Delta on the number of write streams
      int m_deltaNbWriteStreams;

      /// The number of read-write streams
      unsigned int m_nbReadWriteStreams;

      /// Delta on the number of read-write streams
      int m_deltaNbReadWriteStreams;

      /// The number of migrator streams
      unsigned int m_nbMigratorStreams;

      /// Delta on the number of migrator streams
      int m_deltaNbMigratorStreams;

      /// The number of recaller streams
      unsigned int m_nbRecallerStreams;

      /// Delta on the number of recaller streams
      int m_deltaNbRecallerStreams;

      /// available space
      u_signed64 m_freeSpace;

      /// Delta on the available space
      signed64 m_deltaFreeSpace;

      /// Last state update (second since EPOCH)
      u_signed64 m_lastStateUpdate;

      /// Last metrics update (second since EPOCH)
      u_signed64 m_lastMetricsUpdate;

    }; // end FileSystemStatus

  } // end monitoring

} // end castor

#endif // MONITORING_FILESYSTEMSTATUS_HPP
