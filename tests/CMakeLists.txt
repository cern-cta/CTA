cmake_minimum_required (VERSION 2.6)

add_executable(unitTests
  unit_tests.cpp)

target_link_libraries(unitTests
  ctacatalogueunittests
  ctaexceptionunittests
  ctaiounittests
  ctalegacymsgunittests
  ctalogunittests
  ctamessagesunittests
  ctaserverunittests
  ctatapereactorunittests
  ctatapeserverdaemonunittests
  ctatapeserverdriveunittests
  ctatapeserverfileunittests
  ctatapeserverscsiunittests
  ctatapeserverutilsunittests
  ctautilsunittests
  ctacommon
  ctacommonunittests
  ctaobjectstore
  ctaobjectstoreunittests
  ctanameserverunittests
  ctaremotensunittests
  ctaschedulerunittests
  gmock
  gtest
  pthread)
