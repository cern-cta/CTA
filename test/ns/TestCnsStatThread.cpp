/******************************************************************************
 *                      TestThread.cpp
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
 * @(#)$RCSfile: TestThread.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/09/12 16:54:21 $ $Author: itglp $
 *
 * Base class for a multithreaded client for stress tests
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include <sys/types.h> 
#include <sys/stat.h>
#include <math.h>
#include <linux/unistd.h>
#include <iostream>
#include <fstream>
#include <string>

#include "occi.h"
#include "Cns_api.h"
#include "Cns_struct.h"
#include "stager_client_api.h"
#include "castor/Services.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/db/newora/OraCnvSvc.hpp"

#include "TestCnsStatThread.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TestCnsStatThread::TestCnsStatThread() :
  m_procTime(0), m_wallTime(0), m_reqCount(0), m_nbThreads(0) {
  m = new castor::server::Mutex(0);
  m_timeStart.tv_usec = 0;

  // load list of files to stat
  std::ifstream f("dumpfromns");
  if(f.is_open()) {    
    while(!f.eof()) {
      std::string line;
      getline(f, line);
      m_files.push_back(line);
    }
    f.close();
    std::cout << "Dump file read successfully." << std::endl;
  }
  else {
    std::cout << "Couldn't read dump file castordump." << std::endl;
  }
}

//------------------------------------------------------------------------------
// cnsStat
//------------------------------------------------------------------------------
u_signed64 TestCnsStatThread::cnsStat(oracle::occi::Statement* m_cnsStatStatement, std::string filepath)
  throw (castor::exception::Exception) {
  u_signed64 fileid = 0;
  try {
    // Check whether the statements are ok
    //if (0 == m_cnsStatStatement) {
    //  m_cnsStatStatement = createStatement("BEGIN cnsStat(:1, :2); END;");
    //  m_cnsStatStatement->registerOutParam
    //    (2, oracle::occi::OCCICURSOR);
    //}

    // Execute the statement
    m_cnsStatStatement->setString(1, filepath);
    m_cnsStatStatement->executeUpdate();
    //oracle::occi::ResultSet *rs = m_cnsStatStatement->getCursor(2);
    fileid = 1; //(u_signed64)m_cnsStatStatement->getDouble(2);

    // Get the data
    //if(rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
    //  fileid = (u_signed64)rs->getDouble(1);
    //}
    //m_cnsStatStatement->closeResultSet(rs);
  }
  catch (oracle::occi::SQLException e) {
    //castor::exception::Exception ex(serrno);
    //ex.getMessage()
    //  << "Error caught in cnsStat() " 
    //  << std::endl << e.what();
    //throw ex;
  }
  return fileid;
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void TestCnsStatThread::run(void* param) {

  u_signed64 i, c = 0;
  u_signed64 reqProcTime = 0, timeStdDev = 0;
  castor::server::SignalThreadPool* p = (castor::server::SignalThreadPool*)param; 

  //char server[] = "c2itdcns";   // for getpath
  u_signed64 maxNb = (m_files.size() ? m_files.size() : 200000000);
  struct Cns_fileid cnsFileid;
  struct Cns_filestat cnsFilestat;

  std::ostringstream stmt;
  stmt << "BEGIN cnsStatFid"
    << syscall(__NR_gettid) % 2 << "@cnsDbLink(:1, :2); END;";
  castor::db::DbCnvSvc* dbSvc = dynamic_cast<castor::db::DbCnvSvc*>
    (svcs()->service("DbCnvSvc", castor::SVC_DBCNV));
  oracle::occi::Statement* m_cnsStatStatement =
    dynamic_cast<castor::db::ora::OraCnvSvc*>
    (dbSvc)->createOraStatement("BEGIN cnsStatFid@cnsDbLink(:1); END;");   //stmt.str());
  //m_cnsStatStatement->registerOutParam(2, oracle::occi::OCCICURSOR);

  if(!m_timeStart.tv_usec) {
    // hopefully only one thread computes it
    m_timeStart.tv_usec = 1;
    gettimeofday(&m_timeStart, 0);
  }
  
  timeval reqStart, reqEnd;
  while(!p->stopped()) {
    i = rand() % maxNb;
    memset(&cnsFileid, 0, sizeof(cnsFileid));
    memset(&cnsFilestat, 0, sizeof(cnsFilestat));

    gettimeofday(&reqStart, 0);

    // *** Do something here ***
    cnsStat(m_cnsStatStatement, m_files[i]);
    //Cns_statx(m_files[i].c_str(), &cnsFileid, &cnsFilestat);
 
    // collect statistics    
    gettimeofday(&reqEnd, 0);
    c++;
    u_signed64 t = ((reqEnd.tv_sec * 1000000) + reqEnd.tv_usec) - ((reqStart.tv_sec * 1000000) + reqStart.tv_usec);
    reqProcTime += t;
    timeStdDev += t*t;
  }
  // print some statistics for this thread; uncertainty range = 1 sigma
  std::cout.precision(3);
  std::cout << "Thread #" << syscall(__NR_gettid) << " : req.count = " << c << "  avgProcTime = " << reqProcTime * 0.000001/c
            << " sec  +/- " << 0.000001*(c-1)/c*sqrt(timeStdDev*1.0/c - (reqProcTime*1.0/c)*(reqProcTime*1.0/c)) << " sec" << std::endl;
  
  if(!m_wallTime) {
    // hopefully only one thread computes it
    m_wallTime = 1;
    timeval tv;
    gettimeofday(&tv, 0);
    m_wallTime = ((tv.tv_sec * 1000000) + tv.tv_usec) - ((m_timeStart.tv_sec * 1000000) + m_timeStart.tv_usec);
  }
  
  // collect global statistics
  m->lock();
  m_reqCount += c;
  m_nbThreads++;
  m_procTime += reqProcTime;
  m->release();
  
  if(m_nbThreads == p->getNbThreads()) {
    // we're the last one, let's print the overall statistics
    std::cout.precision(4);
    std::cout << "Total req.count = " << m_reqCount << "  wallTime = " << m_wallTime * 0.000001 << " sec  avgProcTime = " << m_procTime * 0.000001/m_reqCount
              << " sec  p/wTime = " << m_procTime/m_wallTime << "  throughput = " << m_reqCount*1000000.0/m_wallTime << " req/sec" << std::endl;
  }
}
