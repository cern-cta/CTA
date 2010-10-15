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

#include "Cns_api.h"
#include "Cns_struct.h"
#include "stager_client_api.h"
#include "serrno.h"
#include "osdep.h"
#include "TestThread.hpp"
#include "castor/server/SignalThreadPool.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TestThread::TestThread() :
  m_procTime(0), m_wallTime(0), m_reqCount(0), m_nbThreads(0) {
  m = new castor::server::Mutex(0);
  m_timeStart.tv_usec = 0;
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void TestThread::run(void* param) {
  int rc, Mv,mv,Mr,mr;

  u_signed64 c = 0;
  u_signed64 reqProcTime = 0, timeStdDev = 0;
  castor::server::SignalThreadPool* p = (castor::server::SignalThreadPool*)param; 

  if(!m_timeStart.tv_usec) {
    // hopefully only one thread computes it
    m_timeStart.tv_usec = 1;
    gettimeofday(&m_timeStart, 0);
  }
  
  timeval reqStart, reqEnd;
  while(!p->stopped()) {

    gettimeofday(&reqStart, 0);

    // *** Do something here ***
    rc = stage_version(&Mv, &mv, &Mr, &mr, NULL);
 
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
