// Includes
#include <list>
#include <limits>
#include <iostream>
#include <qobject.h>
#include <qregexp.h>

// local includes
#include "cppcastorwriter.h"
#include "../attribute.h"
#include "../classifier.h"
#include "../class.h"

//-----------------------------------------------------------------------------
// Implementation file for class : cppcastorwriter
//
// 2004-01-13 : Sebastien Ponce
//-----------------------------------------------------------------------------

//=============================================================================
// Standard constructor, initializes variables
//=============================================================================
CppCastorWriter::CppCastorWriter(UMLDoc *parent,
                                 const char *name) :
  SimpleCodeGenerator(parent, name) {
  // All used castor types
  m_castorTypes[QString("Cuuid")] = QString("\"Cuuid.h\"");
  m_castorIsComplexType[QString("Cuuid")] = true;
  m_castorTypes[QString("u_signed64")] = QString("\"osdep.h\"");
  m_castorIsComplexType[QString("u_signed64")] = false;
  // Types to ignore
  m_ignoreClasses.insert(QString("IPersistent"));
  m_ignoreClasses.insert(QString("IStreamable"));
}

