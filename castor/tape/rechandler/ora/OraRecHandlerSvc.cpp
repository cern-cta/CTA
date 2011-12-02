/******************************************************************************
 *                      OraRecHandlerSvc.cpp
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
 * @(#)OraPolicySvc.cpp,v 1.20 $Release$ 2007/04/13 11:58:53 sponcec3
 *
 * Implementation of the IRecHandlerSvc for Oracle
 *
 * @author Giulia Taurelli
 *****************************************************************************/

// Include Files

#include <iostream>

#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/OutOfMemory.hpp"

#include "castor/stager/SvcClass.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/tape/rechandler/RecallPolicyElement.hpp"

#include "castor/tape/rechandler/ora/OraRecHandlerSvc.hpp"



// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::tape::rechandler::ora::OraRecHandlerSvc>* s_factoryOraRecHandlerSvc =
  new castor::SvcFactory<castor::tape::rechandler::ora::OraRecHandlerSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for resurrectTapes

const std::string castor::tape::rechandler::ora::OraRecHandlerSvc::s_resurrectTapesStatementString = 
  "BEGIN  resurrectTapes(:1);END;";

/// SQL statement for tapesAndMountsForRecallPolicy

const std::string castor::tape::rechandler::ora::OraRecHandlerSvc::s_tapesAndMountsForRecallPolicyStatementString =
  "BEGIN  tapesAndMountsForRecallPolicy(:1,:2);END;";



// -----------------------------------------------------------------------
// OraRecHandlerSvc
// -----------------------------------------------------------------------
castor::tape::rechandler::ora::OraRecHandlerSvc::OraRecHandlerSvc(const std::string name) :
  IRecHandlerSvc(),  
  OraCommonSvc(name), 
  m_resurrectTapesStatement(0),
  m_tapesAndMountsForRecallPolicyStatement(0){
}

// -----------------------------------------------------------------------
// ~OraRecHandlerSvc
// -----------------------------------------------------------------------
castor::tape::rechandler::ora::OraRecHandlerSvc::~OraRecHandlerSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
unsigned int castor::tape::rechandler::ora::OraRecHandlerSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
unsigned int castor::tape::rechandler::ora::OraRecHandlerSvc::ID() {
  return castor::SVC_ORARECHANDLERSVC;
}

//-----------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::tape::rechandler::ora::OraRecHandlerSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if  (m_resurrectTapesStatement) deleteStatement(m_resurrectTapesStatement);
    if  (m_tapesAndMountsForRecallPolicyStatement) deleteStatement(m_tapesAndMountsForRecallPolicyStatement);
  } catch (castor::exception::Exception& ignored) {};
  // Now reset all pointers to 0
  m_resurrectTapesStatement=0;
  m_tapesAndMountsForRecallPolicyStatement=0;
}


//---------------------------------------------------------------------
//    resurrectTapes 
//---------------------------------------------------------------------

void castor::tape::rechandler::ora::OraRecHandlerSvc::resurrectTapes(const std::list<u_signed64>& eligibleTapeIds)
  throw (castor::exception::Exception){
  unsigned char (*buffer)[21] = 0;
  ub2 *lens =0;
  try {
    // Check whether the statements are ok
    if (0 ==  m_resurrectTapesStatement) {
      m_resurrectTapesStatement  =
        createStatement(s_resurrectTapesStatementString); 
      m_resurrectTapesStatement->setAutoCommit(true);
    }
    unsigned int  nb = eligibleTapeIds.size();
    if (nb == 0) return;

    lens = (ub2 *)malloc (sizeof(ub2)*nb);
    buffer=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    
    if ( buffer == 0 || lens == 0 ) {
      if (buffer != 0) free(buffer);
      if (lens != 0) free(lens);
      castor::exception::OutOfMemory e; 
      throw e;
    }

    int i=0;
    std::list<u_signed64>::const_iterator elem=eligibleTapeIds.begin();
    while (elem != eligibleTapeIds.end()){
      oracle::occi::Number n = (double)(*elem);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(buffer[i],b.length());
      lens[i] = b.length();
      elem++;
      i++;
    }
    ub4 unused = nb;
    m_resurrectTapesStatement->setDataBufferArray
      (1, buffer, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lens);
   
    nb= m_resurrectTapesStatement->executeUpdate();

    //free allocated memory needed because of oracle unfriendly interface
    if (lens) {free(lens);lens=0;}
    if (buffer){free(buffer);buffer=0;}

  } catch (oracle::occi::SQLException e) {
    if (lens) {free(lens);lens=0;}
    if (buffer){free(buffer);buffer=0;}
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in resurrectTapes."
      << std::endl << e.what();
    throw ex;
  }

}

//---------------------------------------------------------------------
// tapesAndMountsForRecallPolicy
//---------------------------------------------------------------------

void  castor::tape::rechandler::ora::OraRecHandlerSvc::tapesAndMountsForRecallPolicy(std::list<castor::tape::rechandler::RecallPolicyElement>& candidates, int& nbMountsForRecall) throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok
    if (0 ==  m_tapesAndMountsForRecallPolicyStatement) {
      m_tapesAndMountsForRecallPolicyStatement  =
        createStatement(s_tapesAndMountsForRecallPolicyStatementString);
      m_tapesAndMountsForRecallPolicyStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
      m_tapesAndMountsForRecallPolicyStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);

      m_tapesAndMountsForRecallPolicyStatement->setAutoCommit(true);
    }  
   
    m_tapesAndMountsForRecallPolicyStatement->executeUpdate();

    nbMountsForRecall = (int) m_tapesAndMountsForRecallPolicyStatement->getDouble(2);

    oracle::occi::ResultSet *rs =
      m_tapesAndMountsForRecallPolicyStatement->getCursor(1);
  
    // Run through the cursor
   
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::tape::rechandler::RecallPolicyElement item;
    
      item.tapeId             = (u_signed64)rs->getDouble(1);
      item.vid                = rs->getString(2);
      item.numSegments        = (u_signed64)rs->getDouble(3);
      item.totalBytes         = (u_signed64)rs->getDouble(4);
      item.ageOfOldestSegment = (u_signed64)rs->getDouble(5);
      item.priority           = (u_signed64)rs->getDouble(6);
      item.status             = (u_signed64)rs->getDouble(7);

      candidates.push_back(item);
      status = rs->next();
    }
    m_tapesAndMountsForRecallPolicyStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in  tapesAndMountsForRecallPolicy."
      << std::endl << e.what();
    throw ex;
  }
}
