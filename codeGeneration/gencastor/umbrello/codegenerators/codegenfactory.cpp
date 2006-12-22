/***************************************************************************
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
 *   copyright (C) 2003-2006                                               *
 *   Umbrello UML Modeller Authors <uml-devel@ uml.sf.net>                 *
 ***************************************************************************/

// own header
#include "codegenfactory.h"

// qt/kde includes
#include <qstringlist.h>
#include <kdebug.h>

// app includes
#include "../codegenerator.h"
#include "../umldoc.h"
#include "../uml.h"
#include "../optionstate.h"
#include "../operation.h"
#include "../attribute.h"
#include "../umlrole.h"

#include "cppwriter.h"

#include <kdebug.h>


namespace CodeGenFactory {

CodeGenerator* createObject(Uml::Programming_Language pl)  {
    CodeGenerator* obj = 0;

    if(pl == Uml::pl_Cpp) {
      obj = new CppWriter( );
    } else {
      kdWarning() << "cannot create object of type " << pl
                  << ". Type unknown" << endl;
    }
    obj->initFromParentDocument();
    return obj;
}

CodeDocument * newClassifierCodeDocument (UMLClassifier * c)
{
    return NULL;
}

CodeOperation *newCodeOperation(ClassifierCodeDocument *ccd, UMLOperation * op) {
    CodeOperation *retval = NULL;
    return retval;
}


CodeClassField * newCodeClassField(ClassifierCodeDocument *ccd, UMLAttribute *at) {
    CodeClassField *retval = NULL;
    return retval;
}

CodeClassField * newCodeClassField(ClassifierCodeDocument *ccd, UMLRole *role) {
    CodeClassField *retval = NULL;
    return retval;
}

CodeAccessorMethod * newCodeAccessorMethod(ClassifierCodeDocument *ccd,
                                           CodeClassField *cf,
                                           CodeAccessorMethod::AccessorType type) {
    CodeAccessorMethod *retval = NULL;
    return retval;
}

CodeClassFieldDeclarationBlock * newDeclarationCodeBlock (ClassifierCodeDocument *cd,
                                                          CodeClassField * cf) {
    CodeClassFieldDeclarationBlock *retval = NULL;
    return retval;
}

CodeComment * newCodeComment (CodeDocument *cd) {
    return new CodeComment(cd);
}

}  // end namespace CodeGenFactory

