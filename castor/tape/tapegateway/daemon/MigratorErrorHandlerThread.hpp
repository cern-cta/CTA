
/******************************************************************************
 *                       MigratorErrorHandlerThread.hpp
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
 * @(#)$RCSfile: MigratorErrorHandlerThread.hpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef MIGRATORERRORHANDLER_THREAD_HPP
#define MIGRATORERRORHANDLER_THREAD_HPP  1

#include "castor/server/BaseDbThread.hpp"
#include "castor/tape/tapegateway/RetryPolicyElement.hpp"


namespace castor     {
namespace tape       {
namespace tapegateway{

    /**
     * MigratorErrorHandler tread.
     */

  class MigratorErrorHandlerThread :
    public castor::server::BaseDbThread {
    PyObject* m_pyFunction;
        
    bool applyRetryMigrationPolicy(const RetryPolicyElement& elem)
      throw (castor::exception::Exception );
  public:

    MigratorErrorHandlerThread( PyObject* pyFunction );
    virtual ~MigratorErrorHandlerThread() throw() {};
    virtual void run(void*);
    
  };

} // end of tapegateway
} // end of namespace tape
} // end of namespace castor

#endif //  MIGRATORERRORHANDLER_THREAD_HPP
