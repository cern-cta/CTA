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

QStringList CodeGeneratorFactory::languagesAvailable() {
	kdDebug()<<"Querying languages available"<<endl;

	QStringList l;
	l.append("Cpp");
	return l;
}

QString CodeGeneratorFactory::generatorName(const QString &l) {
	kdDebug()<<"Looking up generator for language "<<l<<endl;
	if (l=="Cpp")
		return "CppWriter";
	//else...
	kdDebug()<<"CodeGeneratorFactory::Error: no generator for language "<<l<<endl;
	return "";
}

CodeGenerator* CodeGeneratorFactory::createObject(UMLDoc* doc, const char* name)  {
	CodeGenerator* obj = 0;
	QString cname(name);

	if (doc) {

    if(cname == "CppWriter") {
      obj = new CppWriter( doc, name );
		} else {
			kdWarning() << "cannot create object of type " << name <<
				". Type unknown" << endl;
		}

	} else {
		kdWarning() << "cannot create parent UML document" << endl;
	}
	return obj;
}
