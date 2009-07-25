/******************************************************************************
 *                 test/castor/tape/tpcp/testtpcp.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/tape/tpcp/TapeFseqRange.hpp"
#include "castor/tape/tpcp/TapeFseqRangeList.hpp"
#include "castor/tape/tpcp/TapeFseqRangeListSequence.hpp"
#include "castor/tape/tpcp/TapeFseqRangeSequence.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <stdint.h>


int testTapeFseqRangeSequence() {
  using namespace castor::tape::tpcp;
  using namespace castor::tape::utils;

  std::ostream &os = std::cout;

  os << std::endl;
  writeBanner(os, __FUNCTION__);
  os << std::endl;

  TapeFseqRange range = {21,29};

  TapeFseqRangeSequence seq0;  // Empty sequence
  TapeFseqRangeSequence seq1(range);

  os << "Expected seq0.hasMore() = false" << std::endl;
  os << "Actual   seq0.hasMore() = " << (seq0.hasMore() ? "true" : "false")
    << std::endl;

  if(seq0.hasMore()) {
    return -1; // Test failed
  }

  uint32_t expected = 0;
  uint32_t actual   = 0;

  for(expected=21 ;expected<=29; expected++) {
    os << "Expected seq1.next() = " << expected << std::endl;
    if(seq1.hasMore()) {
      actual = seq1.next();

      os << "Actual   seq1.next() = " << actual << std::endl;

      if(actual != expected) {
        return -1; // Test failed
      }
    } else {
      os << "Actual NO MORE" << std::endl;
      return -1; // Test failed
    }
  }

  return 0;
}


int testTapeFseqRangeListSequence() {
  using namespace castor::tape;

  std::ostream &os = std::cout;
  
  os << std::endl;
  utils::writeBanner(os, __FUNCTION__);
  os << std::endl;

  castor::tape::tpcp::TapeFseqRangeListSequence emtpySeq;

  os << "Expected emtpySeq.hasMore() = false" << std::endl;
  os << "Actual   emtpySeq.hasMore() = "
     << (emtpySeq.hasMore() ? "true" : "false") << std::endl;

  if(emtpySeq.hasMore()) {
    return -1; // Test failed
  }
  
  castor::tape::tpcp::TapeFseqRange range0 = { 1, 3};
  castor::tape::tpcp::TapeFseqRange range1 = { 4,10};
  castor::tape::tpcp::TapeFseqRange range2 = {21,29};
  castor::tape::tpcp::TapeFseqRange range3 = {30,30};
  
  castor::tape::tpcp::TapeFseqRangeList list;
    
  list.push_back(range0);
  list.push_back(range1);
  list.push_back(range2);
  list.push_back(range3);
  
  castor::tape::tpcp::TapeFseqRangeListSequence seq(&list);

  uint32_t expected = 0;
  uint32_t actual   = 0;

  for(expected=1 ;expected<=10; expected++) {
    os << "Expected seq.next() = " << expected << std::endl;
    if(seq.hasMore()) {
      actual = seq.next();

      os << "Actual   seq.next() = " << actual << std::endl;

      if(actual != expected) {
        return -1; // Test failed
      }
    } else {
      os << "Actual   NO MORE" << std::endl;
      return -1; // Test failed
    }
  }

  for(expected=21 ;expected<=30; expected++) {
    os << "Expected seq.next() = " << expected << std::endl;
    if(seq.hasMore()) {
      actual = seq.next();

      os << "Actual   seq.next() = " << actual << std::endl;

      if(actual != expected) {
        return -1; // Test failed
      }
    } else {
      os << "Actual   NO MORE" << std::endl;
      return -1; // Test failed
    }
  }

  return 0;
}


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char **argv) {

  using namespace castor::tape;

  std::ostream &os = std::cout;
  unsigned int nbTests    = 0;
  unsigned int nbFailed   = 0;
  int          testResult = 0;
  int          endResult  = 0;


  os << std::endl;
  utils::writeBanner(os, "Test tpcp");
  os << std::endl;

  testResult = testTapeFseqRangeSequence();
  nbTests++;
  if(testResult != 0) {
    endResult = testResult;
    nbFailed++;
  }

  testResult = testTapeFseqRangeListSequence();
  nbTests++;
  if(testResult != 0) {
    endResult = testResult;
    nbFailed++;
  }
  
  std::cout << std::endl;
  if(endResult == 0) {
    std::cout << "All " << nbTests << " tests passed" << std::endl;
  } else {
    std::cout << nbFailed << " tests failed out of " << nbTests << std::endl;
  }
  
  return endResult;

}
