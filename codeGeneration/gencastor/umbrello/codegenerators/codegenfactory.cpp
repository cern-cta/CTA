/***************************************************************************
                          codegenfactory.cpp  -  description
                             -------------------
    begin                : Mon Jun 17 2002
    copyright            : (C) 2002 by Luis De la Parra Blum
					and Brian Thomas
    email                : luis@delaparra.org
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

#include "codegenfactory.h"
#include "../codegenerator.h"
#include "../umldoc.h"

// the old
#include "cppwriter.h"

#include "qstringlist.h"
#include <kdebug.h>

CodeGeneratorFactory::CodeGeneratorFactory()  {
	kdDebug()<<"CodeGeneratorFactory created"<<endl;
}

CodeGeneratorFactory::~CodeGeneratorFactory() {
}

CodeGenerator* CodeGeneratorFactory::createObject(Uml::Programming_Language pl) {
  CodeGenerator* obj = 0;

  if(pl == Uml::pl_Cpp) {
    obj = new CppWriter( );
  } else {
    kdWarning() << "cannot create object of type " << pl
                << ". Type unknown" << endl;
  }
  return obj;
}
