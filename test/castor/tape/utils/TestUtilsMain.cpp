
#include "castor/tape/utils/IndexedContainer.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <stdlib.h>


int testIndexedContainer() {
  std::ostream &os = std::cout;

  os << std::endl;
  castor::tape::utils::writeBanner(os, __FUNCTION__);

  castor::tape::utils::IndexedContainer<const void *> c(3);

  os << std::endl;
  os << "Empty c=";
  c.write(os);
  os << std::endl;

  const void *ptr1 = (const void *)0x12;
  const void *ptr2 = (const void *)0x34;
  const void *ptr3 = (const void *)0x56;

  os << std::endl;
  os << "ptr1=" << ptr1 << std::endl;
  os << "ptr2=" << ptr2 << std::endl;
  os << "ptr3=" << ptr3 << std::endl;

  const int ptr1Index = c.insert(ptr1);
  const int ptr2Index = c.insert(ptr2);
  const int ptr3Index = c.insert(ptr3);

  os << std::endl;
  os << "ptr1Index=" << ptr1Index << std::endl;
  os << "ptr2Index=" << ptr2Index << std::endl;
  os << "ptr3Index=" << ptr3Index << std::endl;

  os << std::endl;
  os << "First population of c=";
  c.write(os);
  os << std::endl;

  const void *ptr4 = (const void *)0x78;

  os << std::endl;
  os << "Adding one too many pointers" << std::endl;
  try {
    c.insert(ptr4);
    return(-1); // Test failed
  } catch(castor::exception::OutOfMemory &ex) {
    os << std::endl;
    os << "Correctly caught castor::exception::OutOfMemory: "
          "ex.code()=" << ex.code()
       << " ex.getMessage().str()=\"" << ex.getMessage().str() << "\"";
    os << std::endl;
  }

  os << std::endl;
  os << "Removing an element using the invalid index of -1" << std::endl;
  try {
    c.remove(-1);
    return(-1); // Test failed
  } catch(castor::exception::InvalidArgument &ex) {
    os << std::endl;
    os << "Correctly caught castor::exception::InvalidArgument: "
          "ex.code()=" << ex.code()
       << " ex.getMessage().str()=\"" << ex.getMessage().str() << "\"";
    os << std::endl;
  }

  os << std::endl;
  os << "Removing an element using the invalid index of 3" << std::endl;
  try {
    c.remove(3);
    return(-1); // Test failed
  } catch(castor::exception::InvalidArgument &ex) {
    os << std::endl;
    os << "Correctly caught castor::exception::InvalidArgument: "
          "ex.code()=" << ex.code()
       << " ex.getMessage().str()=\"" << ex.getMessage().str() << "\"";
    os << std::endl;
  }

  os << std::endl;
  os << "Removing element with index 1" << std::endl;
  const void *removedPtr2 = c.remove(1);
  if(removedPtr2 != ptr2) {
    return(-1); // Test failed
  }

  os << std::endl;
  os << "removedPtr2=" << removedPtr2 <<  std::endl;

  os << std::endl;
  os << "After removal c=";
  c.write(os);
  os << std::endl;

  os << std::endl;
  os << "Illegally removing the free element with index 1" << std::endl;
  try {
    c.remove(1);

    os << "Failed to detect the entry is already free" << std::endl;
    os << std::endl;

    return(-1); // Test failed
  } catch(castor::exception::NoEntry &ex) {
    os << std::endl;
    os << "Correctly caught castor::exception::NoEntry: "
          "ex.code()=" << ex.code()
       << " ex.getMessage().str()=\"" << ex.getMessage().str() << "\"";
    os << std::endl;
  }

  os << std::endl;

  return 0;
}


int testToHex() {
  std::ostream &os = std::cout;

  os << std::endl;
  castor::tape::utils::writeBanner(os, __FUNCTION__);

  uint32_t   number          = 3735943886;
  const char *expectedResult = "deadface";

  try {
    char buf[8];
    castor::tape::utils::toHex(number, buf);

    os << std::endl;
    os << "Failed to detect a buffer that is too small" << std::endl;

    return(-1); // Test failed
  } catch(castor::exception::InvalidArgument &ex) {
    os << std::endl;
    os << "Correctly caught castor::exception::InvalidArgument: "
          "ex.code()=" << ex.code()
       << " ex.getMessage().str()=\"" << ex.getMessage().str() << "\"";
    os << std::endl;
  }

  char buf[9];
  castor::tape::utils::toHex(number, buf);

  os << std::endl;
  os << "toHex(" << number << ", buf)=" << buf << std::endl;

  if(strcmp(expectedResult, buf) != 0) {
    os << std::endl;
    os << "Failed: toHex did not give the expected result: " << expectedResult
       << std::endl;
    return(-1); // Test failed
  }

  return 0;
}

int main(int argc, char **argv) {
  unsigned int nbTests  = 0;
  unsigned int nbFailed = 0;


  if(testIndexedContainer()) nbFailed++;
  nbTests++;

  if(testToHex()) nbFailed++;
  nbTests++;

  std::cout << std::endl;
  std::cout << nbFailed << " tests failed out of " << nbTests << std::endl;
  std::cout << std::endl;

  return(nbFailed == 0 ? 0 : -1);
}
