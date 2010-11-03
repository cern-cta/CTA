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
 * @(#)$RCSfile$
 *
 *
 *
 * @author Eric Cano
 *****************************************************************************/

#ifndef SCOPEDTRANSACTION_HPP_
#define SCOPEDTRANSACTION_HPP_

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"

namespace castor {

namespace tape {

namespace tapegateway {

class ScopedTransaction {
public:
    ScopedTransaction(castor::tape::tapegateway::ITapeGatewaySvc * OraSvc):
        m_OraSvc(OraSvc), m_transactionClosed(false) {};
    virtual ~ScopedTransaction();
    void commit () throw (castor::exception::Exception);
    void rollback() throw (castor::exception::Exception);
    void release();
private:
    castor::tape::tapegateway::ITapeGatewaySvc * m_OraSvc;
    bool m_transactionClosed;
};

}

}

}

#endif /* SCOPEDTRANSACTION_HPP_ */
