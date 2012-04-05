/******************************************************************************
 *           castor/tape/tapegateway/ScopedTransaction.cpp
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
 * Helper object for writing safe code with respect to DB transactions
 * Basically calls rollback in the destructor if commit was not called
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "ScopedTransaction.hpp"
#include "castor/dlf/Dlf.hpp"

namespace castor {

  namespace tape {

    namespace tapegateway {

      //------------------------------------------------------------------------------
      // destructor
      //------------------------------------------------------------------------------
      ScopedTransaction::~ScopedTransaction() {
        // Rollback in case of no previous explicit transaction commit or rollback
        // Intercept any exceptions
        try {
          if (!m_transactionClosed) m_OraSvc->rollback();
        } catch (...) {
          // "Failed to rollback"
          castor::dlf::Param params[] = {
            castor::dlf::Param("Message", "Failed to rollback in ScopedTransaction::~ScopedTransaction")
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_ORACLELIB + 26, 1, params);
        }
      };

      // Explicit commit, disengaging the destructor's mechanism
      void ScopedTransaction::commit() throw (castor::exception::Exception) {
        m_OraSvc->commit();
        m_transactionClosed = true;
      }

      // Explicit rollback, disengaging the destructor's mechanism
      void ScopedTransaction::rollback() throw (castor::exception::Exception) {
        m_OraSvc->rollback();
        m_transactionClosed = true;
      }

      // Release: just disengage the auto-rollback
      void ScopedTransaction::release() {
        m_transactionClosed = true;
      }

    } // namespace tapegateway
  } // namespace tape
} // namespace castor
