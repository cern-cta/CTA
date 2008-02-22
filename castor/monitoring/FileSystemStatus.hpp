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

// Include files
#include <map>
#include <iostream>
#include "osdep.h"
#include "castor/stager/FileSystemStatusCodes.hpp"
#include "castor/monitoring/FileSystemRating.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/monitoring/SharedMemoryString.hpp"
#include "castor/monitoring/ClusterStatusBlockKey.hpp"


namespace castor {

  namespace monitoring {

    /*
     * Describes the status of one filesystem
     * Enforces usage of an external memory allocator for the Filesystem list
     */
    class FileSystemStatus :
      public std::map<castor::monitoring::SharedMemoryString,
                      castor::monitoring::FileSystemRating,
                      std::less<castor::monitoring::SharedMemoryString>,
                      castor::sharedMemory::Allocator
                      <std::pair<castor::monitoring::SharedMemoryString,
                                 castor::monitoring::FileSystemRating>,
                       castor::monitoring::ClusterStatusBlockKey> > {

    public:

      /**
       * Constructor
       */
      FileSystemStatus();

      /**
       * print method
       */
      void print(std::ostream& out,
		 const std::string& indentation = "",
		 const bool showAll = false) const
        throw();

    public:

      /// Accessor to space
      u_signed64 space() const {
	return m_space;
      }

      /// Accessor to space
      void setSpace(u_signed64 new_var) {
	m_space = new_var;
      }

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
      void setStatus(castor::stager::FileSystemStatusCodes new_var) {
        m_status = new_var;
      }

      /// Accessor to adminStatus
      castor::monitoring::AdminStatusCodes adminStatus() const {
        return m_adminStatus;
      }

      /// Accessor to adminStatus
      void setAdminStatus(castor::monitoring::AdminStatusCodes new_var) {
        m_adminStatus = new_var;
      }

      /// Accessor to readRate
      u_signed64 readRate() const {
	return m_readRate;
      }

      /// Accessor to readRate
      void setReadRate(u_signed64 new_var) {
	m_readRate = new_var;
      }

      /// Accessor to deltaReadRate
      signed64 deltaReadRate() const {
	return m_deltaReadRate;
      }

      /// Accessor to deltaReadRate
      void setDeltaReadRate(signed64 new_var) {
	m_deltaReadRate = new_var;
      }

      /// Accessor to writeRate
      u_signed64 writeRate() const {
	return m_writeRate;
      }

      /// Accessor to writeRate
      void setWriteRate(u_signed64 new_var) {
	m_writeRate = new_var;
      }

      /// Accessor to deltaWriteRate
      signed64 deltaWriteRate() const {
	return m_deltaWriteRate;
      }

      /// Accessor to deltaWriteRate
      void setDeltaWriteRate(signed64 new_var) {
	m_deltaWriteRate = new_var;
      }

      /// Accessor to nbReadStreams
      unsigned int nbReadStreams() const {
	return m_nbReadStreams;
      }

      /// Accessor to nbReadStreams
      void setNbReadStreams(unsigned int new_var) {
	m_nbReadStreams = new_var;
      }

      /// Accessor to deltaNbReadStreams
      int deltaNbReadStreams() const {
	return m_deltaNbReadStreams;
      }

      /// Accessor to deltaNbReadStreams
      void setDeltaNbReadStreams(int new_var) {
	m_deltaNbReadStreams = new_var;
      }

      /// Accessor to nbWriteStreams
      unsigned int nbWriteStreams() const {
	return m_nbWriteStreams;
      }

      /// Accessor to nbWriteStreams
      void setNbWriteStreams(unsigned int new_var) {
	m_nbWriteStreams = new_var;
      }

      /// Accessor to deltaNbWriteStreams
      int deltaNbWriteStreams() const {
	return m_deltaNbWriteStreams;
      }

      /// Accessor to deltaNbWriteStreams
      void setDeltaNbWriteStreams(int new_var) {
	m_deltaNbWriteStreams = new_var;
      }

      /// Accessor to nbReadWriteStreams
      unsigned int nbReadWriteStreams() const {
	return m_nbReadWriteStreams;
      }

      /// Accessor to nbReadWriteStreams
      void setNbReadWriteStreams(unsigned int new_var) {
	m_nbReadWriteStreams = new_var;
      }

      /// Accessor to deltaNbReadWriteStreams
      int deltaNbReadWriteStreams() const {
	return m_deltaNbReadWriteStreams;
      }

      /// Accessor to deltaNbReadWriteStreams
      void setDeltaNbReadWriteStreams(int new_var) {
	m_deltaNbReadWriteStreams = new_var;
      }

      /// Accessor to nbMigratorStreams
      unsigned int nbMigratorStreams() const {
	return m_nbMigratorStreams;
      }

      /// Accessor to nbMigratorStreams
      void setNbMigratorStreams(unsigned int new_var) {
	m_nbMigratorStreams = new_var;
      }

      /// Accessor to deltaNbMigratorStreams
      int deltaNbMigratorStreams() const {
	return m_deltaNbMigratorStreams;
      }

      /// Accessor to deltaNbMigratorStreams
      void setDeltaNbMigratorStreams(int deltaNbMigratorStreams) {
	m_deltaNbMigratorStreams = deltaNbMigratorStreams;
      }

      /// Accessor to nbRecallerStreams
      unsigned int nbRecallerStreams() const {
	return m_nbRecallerStreams;
      }

      /// Accessor to nbRecallerStreams
      void setNbRecallerStreams(unsigned int new_var) {
	m_nbRecallerStreams = new_var;
      }

      /// Accessor to deltaNbRecallerStreams
      int deltaNbRecallerStreams() const {
	return m_deltaNbRecallerStreams;
      }

      /// Accessor to deltaNbRecallerStreams
      void setDeltaNbRecallerStreams(int new_var) {
	m_deltaNbRecallerStreams = new_var;
      }

      /// Accessor to free space
      u_signed64 freeSpace() const {
	return m_freeSpace;
      }

      /// Accessor to free space
      void setFreeSpace(u_signed64 freeSpace) {
	m_freeSpace = freeSpace;
      }

      /// Accessor to deltaFreeSpace
      signed64 deltaFreeSpace() const {
	return m_deltaFreeSpace;
      }

      /// Accessor to deltaFreeSpace
      void setDeltaFreeSpace(signed64 new_var) {
	m_deltaFreeSpace = new_var;
      }

      /// Accessor to lastStateUpdate
      void setLastStateUpdate(u_signed64 new_var) {
        m_lastStateUpdate = new_var;
      }

      /// Accessor to lastStateUpdate
      u_signed64 lastStateUpdate() {
	return m_lastStateUpdate;
      }

      /// Accessor to lastMetricsUpdate
      void setLastMetricsUpdate(u_signed64 new_var) {
        m_lastMetricsUpdate = new_var;
      }

      /// Accessor to lastMetricsUpdate
      u_signed64 lastMetricsUpdate() {
	return m_lastMetricsUpdate;
      }

      /// Accessor to lastRatingUpdate
      void setLastRatingUpdate(u_signed64 new_var) {
	m_lastRatingUpdate = new_var;
      }

      /// Accessor to lastRatingUpdate
      u_signed64 lastRatingUpdate() const {
	return m_lastRatingUpdate;
      }      

      /// Accessor to lastRatingError
      void setLastRatingError(u_signed64 new_var) {
	m_lastRatingError = new_var;
      }

      /// Accessor to lastRatingError
      u_signed64 lastRatingError() const {
	return m_lastRatingError;
      } 

    private:

      /// Total space
      u_signed64 m_space;

      /**
       * The fraction of free space under which the garbage collector will
       * run. This number must be < 1
       */
      float m_minFreeSpace;

      /**
       * The fraction of free space that the garbage collector is targeting 
       * when liberating space. This number must be < 1
       */
      float m_maxFreeSpace;

      /**
       * The fraction of free space under which we stop scheduling write jobs
       * on the filesystem. This number must be < 1
       */
      float m_minAllowedFreeSpace;

      /// Status
      castor::stager::FileSystemStatusCodes m_status;

      /// Admin status
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

      /// Available space
      u_signed64 m_freeSpace;

      /// Delta on the available space
      signed64 m_deltaFreeSpace;

      /// Last state update (seconds since EPOCH)
      u_signed64 m_lastStateUpdate;

      /// Last metrics update (seconds since EPOCH)
      u_signed64 m_lastMetricsUpdate;

      /// Last rating update (seconds since EPOCH)
      u_signed64 m_lastRatingUpdate;

      /// The last time a rating error occured (seconds since EPOCH)
      u_signed64 m_lastRatingError;

    }; /* end of class FileSystemStatus */

  } /* end of namespace monitoring */

} /* end of namespace castor */

#endif // MONITORING_FILESYSTEMSTATUS_HPP
