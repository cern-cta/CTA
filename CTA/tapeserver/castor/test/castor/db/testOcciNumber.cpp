#include <list>
#include <iostream>
#include <stdint.h>

#include "occi.h"


int main() {
  oracle::occi::Number m_n(1);
  //m_n.setNull();
  const oracle::occi::Number two(2);
  const oracle::occi::Number _32(two.intPower(32));
  try {
    uint32_t n_high = m_n / _32;  // this throws an underflow exception when m_n < _32
    uint32_t n_low  = m_n - oracle::occi::Number(n_high) * _32;
    unsigned long long r = (unsigned long long) (n_high) << 32  | (unsigned long long)  (n_low);
    std::cerr << r << std::endl;
  } catch (oracle::occi::SQLException& oe) {
    std::cerr << "Number exception" << oe.getErrorCode() << std::endl;
    if(oe.getErrorCode() == 22054)  // ORA-22054 = Underflow error
      std::cerr << (uint32_t) m_n << std::endl;
    else
      throw oe;
  }
}