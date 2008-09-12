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
#include "serrno.h"
#include "osdep.h"
#include "TestThread.hpp"
#include "castor/server/ServiceThread.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TestThread::TestThread() :
  m_procTime(0), m_wallTime(0), m_reqCount(0), m_nbThreads(0) {
  m = new castor::server::Mutex(0);
  m_timeStart.tv_usec = 0;
  
  // load list of files to stat
  std::ifstream f("castordump");
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
// run
//------------------------------------------------------------------------------
void TestThread::run(void* param) {
  //char filename[] =    // fileid = 541713
  //  "/castor/cern.ch/cms/reconstruction/muonDIGI0300/THits/muonSignal0300/MB1mu_pt1/EVD32.muonHitPU.s/cern.ch/cms/reconstruction/jetDIGI0300/Digis/1033Pileup/h200_2tau_emu";
  //char filename[] = "/castor/cern.ch/user/i/itglp/stresstest/00000";
  char* filename = NULL;
  u_signed64 i = 0;
  int op;
  char server[] = "c2itdcns";   // for getpath
  u_signed64 maxNb = (m_files.size() ? m_files.size() : 200000000);

  u_signed64 c = 0;
  u_signed64 reqProcTime = 0, timeStdDev = 0;
  struct Cns_fileid cnsFileid;
  struct Cns_filestat cnsFilestat;
  char filepath[CA_MAXPATHLEN + 1];
  
  castor::server::ServiceThread* st = (castor::server::ServiceThread*)param; 

  if(!m_timeStart.tv_usec) {
    // hopefully only one thread computes it
    m_timeStart.tv_usec = 1;
    gettimeofday(&m_timeStart, 0);
  }
  
  timeval reqStart, reqEnd;
  while(!st->stopped()) {
    //filepath[0] = 0;
    //i = rand() % 10000;
    op = rand() % 10;
    i = rand() % maxNb;
    //sprintf(filename + strlen(filename) - 5, "%05lld", i);
    memset(&cnsFileid, 0, sizeof(cnsFileid));
    memset(&cnsFilestat, 0, sizeof(cnsFilestat));

    gettimeofday(&reqStart, 0);

    // *** Do something here ***
    
    //Cns_getpath(server, i, filepath);
    Cns_statx(m_files[i].c_str(), &cnsFileid, &cnsFilestat);
    /*if(op < 6) {
      Cns_statx(filename, &cnsFileid, &cnsFilestat);
    }
    else {
      if(op < 8) {
        Cns_creatx(filename, 0664, &cnsFileid);
      }
      else {
        Cns_unlink(filename);
      }
    }*/
    
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
  
  if(m_nbThreads == st->owner()->getNbThreads()) {
    // we're the last one, let's print the overall statistics
    std::cout.precision(4);
    std::cout << "Total req.count = " << m_reqCount << "  wallTime = " << m_wallTime * 0.000001 << " sec  avgProcTime = " << m_procTime * 0.000001/m_reqCount
              << " sec  p/wTime = " << m_procTime/m_wallTime << "  throughput = " << m_reqCount*1000000.0/m_wallTime << " req/sec" << std::endl;
  }
}
