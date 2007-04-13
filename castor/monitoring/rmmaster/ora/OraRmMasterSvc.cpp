/******************************************************************************
 *                      OraRmMasterSvc.cpp
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
 * @(#)$RCSfile: OraRmMasterSvc.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2007/04/13 11:55:42 $ $Author: sponcec3 $
 *
 * Implementation of the IRmMasterSvc for Oracle
 *
 * @author castor dev team
 *****************************************************************************/

// Include Files
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IClient.hpp"
#include "castor/db/ora/OraRmMasterSvc.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/stager/DiskServer.hpp"
#include "occi.h"
#include <Cuuid.h>
#include <osdep.h>
#include <string>
#include <sstream>
#include <vector>
#include <Cns_api.h>
#include <vmgr_api.h>
#include <Ctape_api.h>
#include <serrno.h>

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraRmMasterSvc>* s_factoryOraRmMasterSvc =
  new castor::SvcFactory<castor::db::ora::OraRmMasterSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for syncClusterStatus
const std::string castor::db::ora::OraRmMasterSvc::s_syncClusterStatusStatementString =
  "BEGIN syncClusterStatus(:1, :2, :3, :4); END;";

const std::string castor::db::ora::OraRmMasterSvc::s_getDiskServersStatementString =
  "SELECT id, name, adminStatus FROM DiskServer";

const std::string castor::db::ora::OraRmMasterSvc::s_getFileSystemsStatementString =
  "SELECT id, mountPoint, adminStatus FROM FileSystem WHERE diskServer = :1";

// -----------------------------------------------------------------------
// OraRmMasterSvc
// -----------------------------------------------------------------------
castor::db::ora::OraRmMasterSvc::OraRmMasterSvc(const std::string name) :
  OraCommonSvc(name),
  m_syncClusterStatusStatement(0), m_getDiskServersStatement(0),
  m_getFileSystemsStatement (0) {}

// -----------------------------------------------------------------------
// ~OraRmMasterSvc
// -----------------------------------------------------------------------
castor::db::ora::OraRmMasterSvc::~OraRmMasterSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraRmMasterSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraRmMasterSvc::ID() {
  return castor::SVC_ORARMMASTERSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraRmMasterSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if(m_syncClusterStatusStatement) deleteStatement(m_syncClusterStatusStatement);
    if(m_getDiskServersStatement) deleteStatement(m_getDiskServersStatement);
    if(m_getFileSystemsStatement) deleteStatement(m_getFileSystemsStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_syncClusterStatusStatement = 0;
  m_getDiskServersStatement = 0;
  m_getFileSystemsStatement = 0;
}

//------------------------------------------------------------------------------
// fillOracleBuffer
//------------------------------------------------------------------------------
inline void fillOracleBuffer(unsigned char buf[][21],
			     ub2* lensBuf,
			     unsigned int index,
			     oracle::occi::Number value) {
  oracle::occi::Bytes b = value.toBytes();
  b.getBytes(buf[index],b.length());
  lensBuf[index] = b.length();
}

// -----------------------------------------------------------------------
// syncClusterStatus
// -----------------------------------------------------------------------
void castor::db::ora::OraRmMasterSvc::syncClusterStatus
(castor::monitoring::ClusterStatus* clusterStatus)
  throw (castor::exception::Exception) {
  if (0 == m_syncClusterStatusStatement) {
    try {
      // Check whether the statements are ok
      m_syncClusterStatusStatement =
	createStatement(s_syncClusterStatusStatementString);
      m_syncClusterStatusStatement->setAutoCommit(true);
    } catch (oracle::occi::SQLException e) {
      handleException(e);
      castor::exception::Internal ex;
      ex.getMessage()
	<< "Unable to create statement for synchronizing DB with monitoring data :"
	<< std::endl << e.getMessage();
      throw ex;
    }
  }
  // Compute array lengths
  unsigned int diskServersL = clusterStatus->size();
  unsigned int fileSystemsL = 0;
  for (castor::monitoring::ClusterStatus::const_iterator it =
	 clusterStatus->begin();
       it != clusterStatus->end();
       it++) {
    fileSystemsL += it->second.size();
  }
  // Since ORACLE does not like 0 length arrays, it's better
  // to protect ourselves and give up in such a case
  if (0 == fileSystemsL) return;
  // Find max length of the string parameters
  ub2 *lensDS = (ub2*) malloc(diskServersL * sizeof(ub2));
  if (0 == lensDS) { castor::exception::OutOfMemory e; throw e; };
  ub2 *lensFS = (ub2*) malloc((diskServersL + fileSystemsL) * sizeof(ub2));
  if (0 == lensFS) { free (lensDS); castor::exception::OutOfMemory e; throw e; };
  unsigned int maxFSL = 0;
  unsigned int maxDSL = 0;
  unsigned int ds = 0;
  unsigned int fs = 0;
  for (castor::monitoring::ClusterStatus::const_iterator it =
	 clusterStatus->begin();
       it != clusterStatus->end();
       it++) {
    lensDS[ds] = it->first.length();
    if (lensDS[ds] > maxDSL) maxDSL = lensDS[ds];
    lensFS[ds+fs] = it->first.length();
    if (lensFS[ds+fs] > maxFSL) maxFSL = lensFS[ds+fs];
    ds++;
    for (castor::monitoring::DiskServerStatus::const_iterator it2
	   = it->second.begin();
	 it2 != it->second.end();
	 it2++) {
      lensFS[ds+fs] = it2->first.length();
      if (lensFS[ds+fs] > maxFSL) maxFSL = lensFS[ds+fs];
      fs++;
    }
  }
  // Allocate buffer for giving the parameters to ORACLE
  unsigned int bufferDSCellSize = maxDSL * sizeof(char);
  char *bufferDS =
    (char*) malloc(diskServersL * bufferDSCellSize);
  if (0 == bufferDS) {
    free (lensDS); free (lensFS);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned int bufferFSCellSize = maxFSL * sizeof(char);
  char *bufferFS =
    (char*) malloc((diskServersL + fileSystemsL) * bufferFSCellSize);
  if (0 == bufferFS) {
    free (lensDS); free (lensFS); free (bufferDS);
    castor::exception::OutOfMemory e; throw e;
  };
  // Put DiskServer and FileSystem names into the buffers
  unsigned int d = 0;
  unsigned int f = 0;
  for (castor::monitoring::ClusterStatus::const_iterator it =
	 clusterStatus->begin();
       it != clusterStatus->end();
       it++) {
    strncpy(bufferDS+(d*bufferDSCellSize), it->first.c_str(), lensDS[d]);
    strncpy(bufferFS+((d+f)*bufferFSCellSize), it->first.c_str(), lensDS[d]);
    d++;
    for (castor::monitoring::DiskServerStatus::const_iterator it2
	   = it->second.begin();
	 it2 != it->second.end();
	 it2++) {
      strncpy(bufferFS+((d+f)*bufferFSCellSize), it2->first.c_str(), lensFS[d+f]);
      f++;
    }
  }
  // Deal with parameters for both DiskServers and FileSystems
  ub2 *lensDSP = (ub2*) malloc(diskServersL * 3 * sizeof(ub2));
  if (0 == lensDSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    castor::exception::OutOfMemory e; throw e;
  };
  ub2 *lensFSP = (ub2*) malloc(fileSystemsL * 9 * sizeof(ub2));
  if (0 == lensFSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    free(lensDSP);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferDSP)[21] =
    (unsigned char(*)[21]) calloc(diskServersL * 3 * 21, sizeof(unsigned char));
  if (0 == bufferDSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    free(lensDSP); free(lensFSP);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferFSP)[21] =
    (unsigned char(*)[21]) calloc(fileSystemsL * 9 * 21, sizeof(unsigned char));
  if (0 == lensFSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    free(lensDSP); free(lensFSP); free(bufferDSP);
    castor::exception::OutOfMemory e; throw e;
  };
  d = 0;
  f = 0;
  try {
    for (castor::monitoring::ClusterStatus::const_iterator it =
	   clusterStatus->begin();
	 it != clusterStatus->end();
	 it++) {
      const castor::monitoring::DiskServerStatus& dss = it->second;
      // fill buffers
      fillOracleBuffer(bufferDSP, lensDSP, 3*d, dss.status());
      fillOracleBuffer(bufferDSP, lensDSP, (3*d)+1, dss.adminStatus());
      fillOracleBuffer(bufferDSP, lensDSP, (3*d)+2, dss.load());
      d++;
      for (castor::monitoring::DiskServerStatus::const_iterator it2
	     = it->second.begin();
	   it2 != dss.end();
	   it2++) {
	const castor::monitoring::FileSystemStatus& fss = it2->second;
	// fill buffers
	fillOracleBuffer(bufferFSP, lensFSP, 9*f, fss.status());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+1, fss.adminStatus());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+2, (double)fss.readRate());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+3, (double)fss.writeRate());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+4, fss.nbReadStreams());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+5, fss.nbWriteStreams());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+6, fss.nbReadWriteStreams());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+7, (double)fss.freeSpace());
	fillOracleBuffer(bufferFSP, lensFSP, (9*f)+8, (double)fss.space());
	f++;
      }
    }
    // prepare the statement
    ub4 DSL = diskServersL;
    ub4 FSL = diskServersL+fileSystemsL;
    ub4 DSPL = 3*diskServersL;
    ub4 FSPL = 9*fileSystemsL;
    m_syncClusterStatusStatement->setDataBufferArray
      (1, bufferDS, oracle::occi::OCCI_SQLT_CHR,
       diskServersL, &DSL, maxDSL, lensDS);
    m_syncClusterStatusStatement->setDataBufferArray
      (2, bufferFS, oracle::occi::OCCI_SQLT_CHR,
       diskServersL+fileSystemsL, &FSL, maxFSL, lensFS);
    m_syncClusterStatusStatement->setDataBufferArray
      (3, bufferDSP, oracle::occi::OCCI_SQLT_NUM,
       3*diskServersL, &DSPL, 21, lensDSP);
    m_syncClusterStatusStatement->setDataBufferArray
      (4, bufferFSP, oracle::occi::OCCI_SQLT_NUM,
       9 * fileSystemsL, &FSPL, 21, lensFSP);
    // Finally execute the statement
    m_syncClusterStatusStatement->executeUpdate();
    // And release the memory
    free(lensDS);
    free(lensFS);
    free(bufferDS);
    free(bufferFS);
    free(lensDSP);
    free(lensFSP);
    free(bufferDSP);
    free(bufferFSP);
  } catch (oracle::occi::SQLException e) {
    // release the memory
    free(lensDS);
    free(lensFS);
    free(bufferDS);
    free(bufferFS);
    free(lensDSP);
    free(lensFSP);
    free(bufferDSP);
    free(bufferFSP);
    // and handle exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to sync DB with monitoring data :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// retrieveClusterStatus
// -----------------------------------------------------------------------
void castor::db::ora::OraRmMasterSvc::retrieveClusterStatus
(castor::monitoring::ClusterStatus* clusterStatus)
  throw (castor::exception::Exception) {
}
