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
  using namespace std;

  ostream &os = cout;

  try {
    os << endl;
    writeBanner(os, __FUNCTION__);
    os << endl;

    try {
      os << "Expecting InvalidArgument exception with range(0, 1)" << endl;
      TapeFseqRange range(0, 1);
      os << "Did not catch the expected InvalidArgument exception" << endl;
      return -1; // Test failed
    } catch(castor::exception::Exception &ex) {
      os << "Caught expected InvalidArgument exception" << endl;
    }

    try {
      os << "Expecting InvalidArgument exception with range(2, 1)" << endl;
      TapeFseqRange range(2, 1);
      os << "Did not catch the expected InvalidArgument exception" << endl;
      return -1; // Test failed
    } catch(castor::exception::Exception &ex) {
      os << "Caught expected InvalidArgument exception" << endl;
    }

    TapeFseqRange emptyRange;

    os << "Checking emptyRange.isEmpty() is true" << endl;
    if(emptyRange.isEmpty()) {
      os << "emptyRange.isEmpty() is true" << endl;
    } else {
      os << "emptyRange.isEmpty() is false" << endl;
      return -1; // Test failed
    }

    os << "Checking emptyRange.lower() is 0" << endl;
    if(emptyRange.lower() == 0) {
      os << "emptyRange.lower() is 0" << endl;
    } else {
      os << "emptyRange.lower() is " << emptyRange.lower() << endl;
      return -1; // Test failed
    }

    os << "Checking emptyRange.upper() is 0" << endl;
    if(emptyRange.upper() == 0) {
      os << "emptyRange.upper() is 0" << endl;
    } else {
      os << "emptyRange.upper() is " << emptyRange.upper() << endl;
      return -1; // Test failed
    }

    os << "Checking emptyRange.size() is 0" << endl;
    if(emptyRange.size() == 0) {
      os << "emptyRange.size() is 0" << endl;
    } else {
      os << "emptyRange.size() is " << emptyRange.size() << endl;
      return -1; // Test failed
    }

    TapeFseqRange from5ToInifinity(5, 0);

    os << "Checking from5ToInifinity.lower() is 5" << endl;
    if(from5ToInifinity.lower() == 5) {
      os << "from5ToInifinity.lower() is 5" << endl;
    } else {
      os << "from5ToInifinity.lower() is " << from5ToInifinity.lower() << endl;
      return -1; // Test failed
    }

    os << "Checking from5ToInifinity.upper() is 0" << endl;
    if(from5ToInifinity.upper() == 0) {
      os << "from5ToInifinity.upper() is 0" << endl;
    } else {
      os << "from5ToInifinity.upper() is " << from5ToInifinity.upper() << endl;
      return -1; // Test failed
    }

    os << "Checking from5ToInifinity.size() is 0" << endl;
    if(from5ToInifinity.size() == 0) {
      os << "from5ToInifinity.size() is 0" << endl;
    } else {
      os << "from5ToInifinity.size() is " << from5ToInifinity.size() << endl;
      return -1; // Test failed
    }

    os << "Checking from5ToInifinity.isEmpty() is false" << endl;
    if(from5ToInifinity.isEmpty()) {
      os << "from5ToInifinity.isEmpty() is true" << endl;
      return -1; // Test failed
    } else {
      os << "from5ToInifinity.isEmpty() is false" << endl;
    }

    TapeFseqRange from7To7(7, 7);

    os << "Checking from7To7.size() is 1" << endl;
    if(from7To7.size() == 1) {
      os << "from7To7.size() is 1" << endl;
    } else {
      os << "from7To7.size() is " << from7To7.size() << endl;
      return -1; // Test failed
    }

    TapeFseqRange range(21, 29);

    os << "Checking range(21, 29).size() is 9" << endl;
    if(range.size() == 9) {
      os << "range(21, 29).size() is 9" << endl;
    } else {
      os << "range(21, 29).size() is " << range.size() << endl;
      return -1; // Test failed
    }

    TapeFseqRangeSequence seq0;  // Empty sequence
    TapeFseqRangeSequence seq1(range);

    os << "Expected seq0.hasMore() = false" << endl;
    os << "Actual   seq0.hasMore() = " << (seq0.hasMore() ? "true" : "false")
       << endl;

    if(seq0.hasMore()) {
      return -1; // Test failed
    }

    uint32_t expected = 0;
    uint32_t actual   = 0;

    for(expected=21 ;expected<=29; expected++) {
      os << "Expected seq1.next() = " << expected << endl;
      if(seq1.hasMore()) {
        actual = seq1.next();

        os << "Actual   seq1.next() = " << actual << endl;

        if(actual != expected) {
          return -1; // Test failed
        }
      } else {
        os << "Actual NO MORE" << endl;
        return -1; // Test failed
      }
    }
  } catch(castor::exception::Exception &ex) {
    os << "Caught an unexpected exception: " << ex.getMessage().str() << endl;
    return -1; // Test failed
  }

  return 0;
}


int testTapeFseqRangeListSequence() {
  using namespace castor::tape;
  using namespace std;

  std::ostream &os = std::cout;
  
  os << std::endl;
  utils::writeBanner(os, __FUNCTION__);
  os << std::endl;

  castor::tape::tpcp::TapeFseqRangeListSequence emtpySeq;

  os << "Checking emtpySeq.isFinite() is true" << endl;
  if(emtpySeq.isFinite()) {
    os << "emtpySeq.isFinite() is true" << endl;
  } else {
    os << "emtpySeq.isFinite() is false" << endl;
    return -1; // Test failed
  }

  os << "Checking emtpySeq.totalSize() is 0" << endl;
  if(emtpySeq.totalSize() == 0) {
    os << "emtpySeq.totalSize() is 0" << endl;
  } else {
    os << "emtpySeq.totalSize() is " << emtpySeq.totalSize() << endl;
    return -1; // Test failed
  }

  os << "Expected emtpySeq.hasMore() = false" << std::endl;
  os << "Actual   emtpySeq.hasMore() = "
     << (emtpySeq.hasMore() ? "true" : "false") << std::endl;

  if(emtpySeq.hasMore()) {
    return -1; // Test failed
  }

  castor::tape::tpcp::TapeFseqRange from7To9(7, 9);
  castor::tape::tpcp::TapeFseqRangeList from7To9List;
  from7To9List.push_back(from7To9);
  castor::tape::tpcp::TapeFseqRangeListSequence from7To9Seq(&from7To9List);

  os << "Checking from7To9Seq.isFinite() is true" << endl;
  if(from7To9Seq.isFinite()) {
    os << "from7To9Seq.isFinite() is true" << endl;
  } else {
    os << "from7To9Seq.isFinite() is false" << endl;
    return -1; // Test failed
  }

  os << "Checking from7To9Seq.totalSize() is 3" << endl;
  if(from7To9Seq.totalSize() == 3) {
    os << "from7To9Seq.totalSize() is 3" << endl;
  } else {
    os << "from7To9Seq.totalSize() is " << from7To9Seq.totalSize() << endl;
    return -1; // Test failed
  }

  castor::tape::tpcp::TapeFseqRange from7ToInfinity(7, 0);
  castor::tape::tpcp::TapeFseqRangeList from7ToInfinityList;
  from7ToInfinityList.push_back(from7ToInfinity);
  castor::tape::tpcp::TapeFseqRangeListSequence
    from7ToInfinitySeq(&from7ToInfinityList);

  os << "Checking from7ToInfinitySeq.isFinite() is false" << endl;
  if(from7ToInfinitySeq.isFinite()) {
    os << "from7ToInfinitySeq.isFinite() is true" << endl;
    return -1; // Test failed
  } else {
    os << "from7ToInfinitySeq.isFinite() is false" << endl;
  }

  os << "Checking from7ToInfinitySeq.totalSize() is 0" << endl;
  if(from7ToInfinitySeq.totalSize() == 0) {
    os << "from7ToInfinitySeq.totalSize() is 0" << endl;
  } else {
    os << "from7ToInfinitySeq.totalSize() is "
       << from7ToInfinitySeq.totalSize() << endl;
    return -1; // Test failed
  }
  
  castor::tape::tpcp::TapeFseqRange range0( 1, 3);
  castor::tape::tpcp::TapeFseqRange range1( 4,10);
  castor::tape::tpcp::TapeFseqRange range2(21,29);
  castor::tape::tpcp::TapeFseqRange range3(30,30);
  
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
