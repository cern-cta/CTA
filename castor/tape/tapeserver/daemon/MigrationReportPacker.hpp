/******************************************************************************
 *                      MigrationReportPacker.hpp
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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/tape/tapeserver/daemon/MockTapeGateway.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/MigrationJob.hpp"
#include <list>

class MigrationReportPacker {
public:
  MigrationReportPacker(MockTapeGateway & tg): m_workerThread(*this),
          m_tapeGateway(tg) {}
  void reportCompletedJob(MigrationJob mj) {
    Report rep;
    rep.migrationJob = mj;
    castor::tape::threading::MutexLocker ml(&m_producterProtection);
    m_fifo.push(rep);
  }
  void reportFlush() {
    Report rep;
    rep.flush = true;
    castor::tape::threading::MutexLocker ml(&m_producterProtection);
    m_fifo.push(rep);
  }
  void reportEndOfSession() {
    Report rep;
    rep.EoSession = true;
    castor::tape::threading::MutexLocker ml(&m_producterProtection);
    m_fifo.push(rep);
  }
  void startThreads() { m_workerThread.start(); }
  void waitThread() { m_workerThread.wait(); }
  virtual ~MigrationReportPacker() { castor::tape::threading::MutexLocker ml(&m_producterProtection); }
private:
  class Report {
  public:
    Report(): flush(false), EoSession(false), 
	    migrationJob(-1, -1, -1) {}
    bool flush;
    bool EoSession;
    MigrationJob migrationJob;
  };
  class WorkerThread: public castor::tape::threading::Thread {
  public:
    WorkerThread(MigrationReportPacker& parent): m_parent(parent) {}
    void run() {
      while(1) {
	MigrationReportPacker::Report rep = m_parent.m_fifo.pop();
	if (rep.flush || rep.EoSession) {
	  m_parent.m_tapeGateway.reportMigratedFiles(m_parent.m_currentReport);
	  m_parent.m_currentReport.clear();
	  if (rep.EoSession) {
	    m_parent.m_tapeGateway.reportEndOfSession(false);
	    return;
	  }
	} else {
	  m_parent.m_currentReport.push_back(rep.migrationJob);
	}
      }
    }
    MigrationReportPacker & m_parent;
  } m_workerThread;
  friend class WorkerThread;
  MockTapeGateway & m_tapeGateway;
  castor::tape::threading::BlockingQueue<Report> m_fifo;
  std::list<MigrationJob> m_currentReport;
  castor::tape::threading::Mutex m_producterProtection;
};
