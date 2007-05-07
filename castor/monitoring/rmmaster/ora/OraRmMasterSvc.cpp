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
 * @(#)$RCSfile: OraRmMasterSvc.cpp,v $ $Revision: 1.11 $ $Release$ $Date: 2007/05/07 16:30:28 $ $Author: sponcec3 $
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
#include "castor/monitoring/rmmaster/ora/OraRmMasterSvc.hpp"
#include "castor/monitoring/rmmaster/ora/StatusUpdateHelper.hpp"
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
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/monitoring/DiskServerStateReport.hpp"
#include "castor/monitoring/FileSystemStateReport.hpp"
#include "occi.h"
#include <Cuuid.h>
#include <osdep.h>
#include <string>
#include <sstream>
#include <vector>
#include <serrno.h>

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::monitoring::rmmaster::ora::OraRmMasterSvc>* s_factoryOraRmMasterSvc =
  new castor::SvcFactory<castor::monitoring::rmmaster::ora::OraRmMasterSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for storeClusterStatus
const std::string castor::monitoring::rmmaster::ora::OraRmMasterSvc::s_storeClusterStatusStatementString =
  "BEGIN storeClusterStatus(:1, :2, :3, :4); END;";

const std::string castor::monitoring::rmmaster::ora::OraRmMasterSvc::s_getDiskServersStatementString =
  "SELECT id, name, adminStatus FROM DiskServer";

const std::string castor::monitoring::rmmaster::ora::OraRmMasterSvc::s_getFileSystemsStatementString =
  "SELECT mountPoint, adminStatus FROM FileSystem WHERE diskServer = :1";

// -----------------------------------------------------------------------
// OraRmMasterSvc
// -----------------------------------------------------------------------
castor::monitoring::rmmaster::ora::OraRmMasterSvc::OraRmMasterSvc(const std::string name) :
  OraCommonSvc(name),
  m_storeClusterStatusStatement(0), m_getDiskServersStatement(0),
  m_getFileSystemsStatement (0) {}

// -----------------------------------------------------------------------
// ~OraRmMasterSvc
// -----------------------------------------------------------------------
castor::monitoring::rmmaster::ora::OraRmMasterSvc::~OraRmMasterSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::monitoring::rmmaster::ora::OraRmMasterSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::monitoring::rmmaster::ora::OraRmMasterSvc::ID() {
  return castor::SVC_ORARMMASTERSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::ora::OraRmMasterSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if(m_storeClusterStatusStatement) deleteStatement(m_storeClusterStatusStatement);
    if(m_getDiskServersStatement) deleteStatement(m_getDiskServersStatement);
    if(m_getFileSystemsStatement) deleteStatement(m_getFileSystemsStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_storeClusterStatusStatement = 0;
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
// storeClusterStatus
// -----------------------------------------------------------------------
void castor::monitoring::rmmaster::ora::OraRmMasterSvc::storeClusterStatus
(castor::monitoring::ClusterStatus* clusterStatus)
  throw (castor::exception::Exception) {
  if (0 == m_storeClusterStatusStatement) {
    try {
      // Check whether the statements are ok
      m_storeClusterStatusStatement =
        createStatement(s_storeClusterStatusStatementString);
      m_storeClusterStatusStatement->setAutoCommit(true);
    } catch (oracle::occi::SQLException e) {
      handleException(e);
      castor::exception::Internal ex;
      ex.getMessage()
      << "Unable to create statement for storing monitoring data to DB :"
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
    // don't send FileSystems for deleted nodes
    if (it->second.adminStatus() != castor::monitoring::ADMIN_DELETED) {
      fileSystemsL += it->second.size();
    }
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
  unsigned int ds = 0;  // diskserver index
  unsigned int dfs = 0; // diskserver index within filesystems list
  unsigned int fs = 0;  // filesystem index (diskserver ignored)
  for (castor::monitoring::ClusterStatus::const_iterator it =
	 clusterStatus->begin();
       it != clusterStatus->end();
       it++) {
    lensDS[ds] = it->first.length();
    if (lensDS[ds] > maxDSL) maxDSL = lensDS[ds];
    ds++;
    // don't send FileSystems for deleted nodes
    if (it->second.adminStatus() != castor::monitoring::ADMIN_DELETED) {
      lensFS[dfs+fs] = it->first.length();
      if (lensFS[dfs+fs] > maxFSL) maxFSL = lensFS[dfs+fs];
      dfs++;
      for (castor::monitoring::DiskServerStatus::const_iterator it2
	     = it->second.begin();
	   it2 != it->second.end();
	   it2++) {
	lensFS[dfs+fs] = it2->first.length();
	if (lensFS[dfs+fs] > maxFSL) maxFSL = lensFS[dfs+fs];
	fs++;
      }
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
  unsigned int d = 0;  // diskserver index
  unsigned int df = 0; // diskserver index within filesystems list
  unsigned int f = 0;  // filesystem index (diskserver ignored)
  for (castor::monitoring::ClusterStatus::const_iterator it =
	 clusterStatus->begin();
       it != clusterStatus->end();
       it++) {
    strncpy(bufferDS+(d*bufferDSCellSize), it->first.c_str(), lensDS[d]);
    d++;
    // don't send FileSystems for deleted nodes
    if (it->second.adminStatus() != castor::monitoring::ADMIN_DELETED) {
      strncpy(bufferFS+((df+f)*bufferFSCellSize), it->first.c_str(), lensDS[d-1]);
      df++;
      for (castor::monitoring::DiskServerStatus::const_iterator it2
	     = it->second.begin();
	   it2 != it->second.end();
	   it2++) {
	strncpy(bufferFS+((df+f)*bufferFSCellSize), it2->first.c_str(), lensFS[df+f]);
	f++;
      }
    }
  }
  // Deal with parameters for both DiskServers and FileSystems
  ub2 *lensDSP = (ub2*) malloc(diskServersL * 9 * sizeof(ub2));
  if (0 == lensDSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    castor::exception::OutOfMemory e; throw e;
  };
  ub2 *lensFSP = (ub2*) malloc(fileSystemsL * 14 * sizeof(ub2));
  if (0 == lensFSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    free(lensDSP);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferDSP)[21] =
    (unsigned char(*)[21]) calloc(diskServersL * 9 * 21, sizeof(unsigned char));
  if (0 == bufferDSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    free(lensDSP); free(lensFSP);
    castor::exception::OutOfMemory e; throw e;
  };
  unsigned char (*bufferFSP)[21] =
    (unsigned char(*)[21]) calloc(fileSystemsL * 14 * 21, sizeof(unsigned char));
  if (0 == lensFSP) {
    free(lensDS); free(lensFS); free(bufferDS); free(bufferFS);
    free(lensDSP); free(lensFSP); free(bufferDSP);
    castor::exception::OutOfMemory e; throw e;
  };
  d = 0;
  df = 0;
  f = 0;
  try {
    for (castor::monitoring::ClusterStatus::const_iterator it =
	   clusterStatus->begin();
	 it != clusterStatus->end();
	 it++) {
      const castor::monitoring::DiskServerStatus& dss = it->second;
      // fill buffers
      fillOracleBuffer(bufferDSP, lensDSP, 9*d, dss.status());
      fillOracleBuffer(bufferDSP, lensDSP, (9*d)+1, dss.adminStatus());
      fillOracleBuffer(bufferDSP, lensDSP, (9*f)+2, (double)dss.readRate());
      fillOracleBuffer(bufferDSP, lensDSP, (9*f)+3, (double)dss.writeRate());
      fillOracleBuffer(bufferDSP, lensDSP, (9*f)+4, dss.nbReadStreams());
      fillOracleBuffer(bufferDSP, lensDSP, (9*f)+5, dss.nbWriteStreams());
      fillOracleBuffer(bufferDSP, lensDSP, (9*f)+6, dss.nbReadWriteStreams());
      fillOracleBuffer(bufferDSP, lensDSP, (9*f)+7, dss.nbMigratorStreams());
      fillOracleBuffer(bufferDSP, lensDSP, (9*f)+8, dss.nbRecallerStreams());
      d++;
      // don't send FileSystems for deleted nodes
      if (it->second.adminStatus() != castor::monitoring::ADMIN_DELETED) {
	for (castor::monitoring::DiskServerStatus::const_iterator it2
	       = it->second.begin();
	     it2 != dss.end();
	     it2++) {
	  const castor::monitoring::FileSystemStatus& fss = it2->second;
	  // fill buffers
	  fillOracleBuffer(bufferFSP, lensFSP, 14*f, fss.status());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+1, fss.adminStatus());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+2, (double)fss.readRate());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+3, (double)fss.writeRate());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+4, fss.nbReadStreams());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+5, fss.nbWriteStreams());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+6, fss.nbReadWriteStreams());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+7, fss.nbMigratorStreams());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+8, fss.nbRecallerStreams());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+9, (double)fss.freeSpace());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+10,(double)fss.space());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+11,(double)fss.minFreeSpace());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+12,(double)fss.maxFreeSpace());
	  fillOracleBuffer(bufferFSP, lensFSP, (14*f)+13,(double)fss.minAllowedFreeSpace());
	  f++;
	}
      }
    }
    // prepare the statement
    ub4 DSL = diskServersL;
    ub4 FSL = diskServersL+fileSystemsL;
    ub4 DSPL = 9*diskServersL;
    ub4 FSPL = 14*fileSystemsL;
    m_storeClusterStatusStatement->setDataBufferArray
      (1, bufferDS, oracle::occi::OCCI_SQLT_CHR,
       diskServersL, &DSL, maxDSL, lensDS);
    m_storeClusterStatusStatement->setDataBufferArray
      (2, bufferFS, oracle::occi::OCCI_SQLT_CHR,
       diskServersL+fileSystemsL, &FSL, maxFSL, lensFS);
    m_storeClusterStatusStatement->setDataBufferArray
      (3, bufferDSP, oracle::occi::OCCI_SQLT_NUM,
       9*diskServersL, &DSPL, 21, lensDSP);
    m_storeClusterStatusStatement->setDataBufferArray
      (4, bufferFSP, oracle::occi::OCCI_SQLT_NUM,
       14 * fileSystemsL, &FSPL, 21, lensFSP);
    // Finally execute the statement
    m_storeClusterStatusStatement->executeUpdate();
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
      << "Unable to store DB with monitoring data :"
      << std::endl << e.getMessage();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// retrieveClusterStatus
// -----------------------------------------------------------------------
void castor::monitoring::rmmaster::ora::OraRmMasterSvc::retrieveClusterStatus
(castor::monitoring::ClusterStatus* clusterStatus)
  throw (castor::exception::Exception) {
  // init statements
  if (0 == m_getDiskServersStatement) {
    try {
      m_getDiskServersStatement = createStatement(s_getDiskServersStatementString);
      m_getDiskServersStatement->setAutoCommit(true);
      m_getFileSystemsStatement = createStatement(s_getFileSystemsStatementString);
      m_getFileSystemsStatement->setAutoCommit(true);
    } catch (oracle::occi::SQLException e) {
      handleException(e);
      castor::exception::Internal ex;
      ex.getMessage()
        << "Unable to create statements for retrieving monitoring data from DB :"
        << std::endl << e.getMessage();
      throw ex;
    }
  }
  
  StatusUpdateHelper* updater = 0; 
  try {
    // init the Status Update helper (shared with RmMasterDaemon/CollectorThread)
    updater = new StatusUpdateHelper(clusterStatus); 
  
    // look for all DiskServers
    oracle::occi::ResultSet *dsRset = m_getDiskServersStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != dsRset->next()) {
      // create a state report for each diskserver
      castor::monitoring::DiskServerStateReport* dsReport =
        new castor::monitoring::DiskServerStateReport();
      dsReport->setName(dsRset->getString(2));
      // by default we start with everything disabled, when the node comes up rmNode
      // will send a full report reenabling the node
      dsReport->setStatus(castor::stager::DISKSERVER_DISABLED);
      // make sure an ADMIN_NONE status in db resets anything found before
      castor::monitoring::AdminStatusCodes adStatus =
        (castor::monitoring::AdminStatusCodes)dsRset->getInt(3);
      if(adStatus == castor::monitoring::ADMIN_NONE)
        adStatus = castor::monitoring::ADMIN_RELEASE;
      dsReport->setAdminStatus(adStatus);

      // loop on its FileSystems
      m_getFileSystemsStatement->setDouble(1, dsRset->getDouble(1));
      oracle::occi::ResultSet *fsRset = m_getFileSystemsStatement->executeQuery();
      while (oracle::occi::ResultSet::END_OF_FETCH != fsRset->next()) {
        castor::monitoring::FileSystemStateReport* fs =
          new castor::monitoring::FileSystemStateReport();
        fs->setMountPoint(fsRset->getString(1));
        fs->setStatus(castor::stager::FILESYSTEM_DISABLED);
        adStatus = (castor::monitoring::AdminStatusCodes)fsRset->getInt(2);
        if(adStatus == castor::monitoring::ADMIN_NONE)
          adStatus = castor::monitoring::ADMIN_RELEASE;
        fs->setAdminStatus(adStatus);
        dsReport->addFileSystemStatesReports(fs);
      }
      
      // "send" the report to update the cluster status
      updater->handleStateUpdate(dsReport);
      delete dsReport;
    }
    delete updater;
  } catch (oracle::occi::SQLException e) {
    if(updater)
      delete updater;
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Unable to retrieve cluster status from DB :"
      << std::endl << e.getMessage();
    throw ex;
  }
  // forward other exceptions (typically from handleStateUpdate) to the caller
}
