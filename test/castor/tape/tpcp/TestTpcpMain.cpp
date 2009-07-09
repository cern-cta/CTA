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
#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <stdint.h>



int testTapeFseqRangeListSequence() {
  using namespace castor::tape;

  std::ostream &os = std::cout;
  
  os << std::endl;
  utils::writeBanner(os, __FUNCTION__);
  os << std::endl;
  
  castor::tape::tpcp::TapeFseqRange range0 = {1,3};
  castor::tape::tpcp::TapeFseqRange range1 = {4,10};
  castor::tape::tpcp::TapeFseqRange range2 = {21,29};
  castor::tape::tpcp::TapeFseqRange range3 = {30,30};
  
  castor::tape::tpcp::TapeFseqRangeList list;
    
  list.push_back(range0);
  list.push_back(range1);
  list.push_back(range2);
  list.push_back(range3);
  
  castor::tape::tpcp::TapeFseqRangeListSequence seq(list);

  uint32_t expected = 0;
  uint32_t actual   = 0;

  for(expected=1 ;expected<=10; expected++) {
    if(seq.hasMore()) {
      actual = seq.next();

      os << "seq.next() = " << actual << std::endl;

      if(actual != expected) {
        return -1; // Test failed
      }
    } else {
      return -1; // Test failed
    }
  }

  for(expected=21 ;expected<=30; expected++) {
    if(seq.hasMore()) {
      actual = seq.next();

      os << "seq.next() = " << actual << std::endl;

      if(actual != expected) {
        return -1; // Test failed
      }
    } else {
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
