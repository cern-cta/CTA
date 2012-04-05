/******************************************************************************
 *           castor/tape/tapegateway/ScopedTransaction.hpp
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

#ifndef SCOPEDTRANSACTION_HPP_
#define SCOPEDTRANSACTION_HPP_

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"

namespace castor {

  namespace tape {

    namespace tapegateway {

      /*
       * Helper object for writing safe code with respect to DB transactions
       * Basically calls rollback in the destructor if commit was not called
       * XXX This should be generalized to any object implementing some
       * XXX transaction interface (to be defined).
       */
      class ScopedTransaction {

      public:

        /* constructor
         * @param OraSvc the underlying ITapeGatewaySvc
         */
        ScopedTransaction(castor::tape::tapegateway::ITapeGatewaySvc * OraSvc):
          m_OraSvc(OraSvc), m_transactionClosed(false) {};

        /* destructor */
        virtual ~ScopedTransaction();

        /* commits the transaction */
        void commit() throw (castor::exception::Exception);

        /* explicitely rollbacks the transaction */
        void rollback() throw (castor::exception::Exception);

        /* pretends the transaction is over, without commitint neither rolling back 
         * XXX should be dropped !
         */
        void release();

      private:

        /* the underlying ITapeGatewaySvc object */
        castor::tape::tapegateway::ITapeGatewaySvc * m_OraSvc;

        /* whether the transaction was closed */
        bool m_transactionClosed;

      };
    } // namespace tapegateway
  } // namespace tape
} // namespace castor

#endif /* SCOPEDTRANSACTION_HPP_ */
