#include "castor/tape/tapegateway/daemon/MountIdToFseqMap.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <stdlib.h>


int testMountIdToFseqMap() {
  using namespace castor::tape;

  std::ostream &os = std::cout;

  os << std::endl;
  utils::writeBanner(os, __FUNCTION__);

  tapegateway::daemon::MountIdToFseqMap map;

  map.insert(1,1);

  try {
    map.insert(1,1);

    std::cerr <<
      "Failed to catch expected exception"
      ": Inserted the same mount transaction id twice" <<
      std::endl;

    return -1; // Return failed
  } catch(castor::exception::Exception &ex) {
    os <<
      "Successfully caught expected exception"
      ": " << ex.getMessage().str() <<
      std::endl;
  }

  int expectedResult = 1;
  int actualResult   = map.nextFseq(1);

  if(actualResult != expectedResult) {
    std::cerr <<
      "Failed map.nextFseq() call"
      ": expectedResult=" << expectedResult <<
      ": actualResult=" << actualResult;

    return -1; // Return failed
  }

  os <<
    "Sucessfully got the next FSEQ of transaction id 1"
    ": fseq=" << actualResult <<
    std::endl;

  expectedResult = 2;
  actualResult   = map.nextFseq(1);

  if(actualResult != expectedResult) {
    std::cerr <<
      "Failed map.nextFseq() call"
      ": expectedResult=" << expectedResult <<
      ": actualResult=" << actualResult;

    return -1; // Return failed
  }

  os <<
    "Sucessfully got the next FSEQ of transaction id 1"
    ": fseq=" << actualResult <<
    std::endl;

  try {
    map.nextFseq(2);

    std::cerr <<
      "Failed to catch expected exception"
      ": Requested next FSEQ of non-existent mount transaction id 2" <<
      std::endl;

    return -1; // Return failed
  } catch(castor::exception::Exception &ex) {
    os <<
      "Successfully caught expected exception"
      ": " << ex.getMessage().str() <<
      std::endl;
  }

  // Return success (-1 is used to  indicate failure)
  return 0;
}


int main() {
  unsigned int nbTests    = 0;
  unsigned int nbFailed   = 0;


  if(testMountIdToFseqMap()) nbFailed++;
  nbTests++;

  std::cout << std::endl;
  std::cout << nbFailed << " tests failed out of " << nbTests << std::endl;
  std::cout << std::endl;

  return(nbFailed == 0 ? 0 : -1);
}
