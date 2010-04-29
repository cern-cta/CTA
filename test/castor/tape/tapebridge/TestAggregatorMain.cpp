#include "castor/tape/tapebridge/SynchronizedCounter.hpp"
#include "castor/tape/tapebridge/VmgrTxRx.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <stdlib.h>


int testVmgrTxRx() {
  using namespace castor::tape;

  std::ostream &os = std::cout;

  os << std::endl;
  utils::writeBanner(os, __FUNCTION__);

  const uint32_t    volReqId            =  0;
  const int         netReadWriteTimeout = 55;
  const uint32_t    uid                 = getuid();
  const uint32_t    gid                 = getgid();
  const char *const vid                 = "I10488";
  legacymsg::VmgrTapeInfoMsgBody tapeInfo;
  utils::setBytes(tapeInfo, '\0');

  try {
    tapebridge::VmgrTxRx::getTapeInfoFromVmgr(nullCuuid, volReqId,
      netReadWriteTimeout, uid, gid, vid, tapeInfo);
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "Call to getTapeInfoFromVmgr() failed"
      ": " << ex.getMessage().str();

    return(-1); // Failed
  }

  // Return success (-1 is used to  indicate failure)
  return 0;
}


int testSynchronisedCounter() {
  using namespace castor::tape;

  std::ostream &os = std::cout;

  os << std::endl;
  utils::writeBanner(os, __FUNCTION__);

  const int initialValue = 5;

  tapebridge::SynchronizedCounter counter(initialValue);

  int expectedValue = initialValue + 1;
  int actualValue = counter.next();
  if(actualValue != expectedValue) {
    std::cerr <<
       "Unexpected value returned by counter.next()"
       ": actualValue=" << actualValue <<
       ": expectedValue=" << expectedValue;

    return(-1); // Failed
  }

  // Return success (-1 is used to  indicate failure)
  return 0;
}


int main() {
  unsigned int nbTests    = 0;
  unsigned int nbFailed   = 0;


  if(testVmgrTxRx()) nbFailed++;
  nbTests++;

  std::cout << std::endl;
  std::cout << nbFailed << " tests failed out of " << nbTests << std::endl;
  std::cout << std::endl;

  return(nbFailed == 0 ? 0 : -1);
}
