/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
#include "package.h"
#include "classifier.h"
#include "association.h"
#include "umlassociationlist.h"
#include "operation.h"
#include "attribute.h"
#include "stereotype.h"
#include "clipboard/idchangelog.h"
#include "umldoc.h"
#include "uml.h"
#include <kdebug.h>
#include <klocale.h>

UMLClassifier::UMLClassifier(const QString & name, int id)
  : UMLPackage(name, id)
{
	init();
}


////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifier::~UMLClassifier() {
}

UMLOperation * UMLClassifier::checkOperationSignature( QString name,
						       UMLAttributeList *opParams,
						       UMLOperation *exemptOp)
{
	UMLObjectList list = findChildObject( Uml::ot_Operation, name );
	if( list.count() == 0 )
		return NULL;
	int inputParmCount = (opParams ? opParams->count() : 0);

	// there is at least one operation with the same name... compare the parameter list
	for( UMLOperation *test = dynamic_cast<UMLOperation*>(list.first());
	     test != 0;
	     test = dynamic_cast<UMLOperation*>(list.next()) )
	{
		if (test == exemptOp)
			continue;
		UMLAttributeList *testParams = test->getParmList( );
		if (!opParams) {
			if (0 == testParams->count())
				return test;
			continue;
		}
		int pCount = testParams->count();
		if( pCount != inputParmCount )
			continue;
		int i = 0;
		for( ; i < pCount; ++i )
		{
			// The only criterion for equivalence is the parameter types.
			// (Default values are not considered.)
			if( testParams->at(i)->getTypeName() != opParams->at(i)->getTypeName() )
				break;
		}
		if( i == pCount )
		{//all parameters matched -> the signature is not unique
			return test;
		}
	}
	// we did not find an exact match, so the signature is unique ( acceptable )
	return NULL;
}

bool UMLClassifier::addOperation(UMLOperation* op, int position )
{
	if (m_OpsList.findRef(op) != -1) {
		kdDebug() << "UMLClassifier::addOperation: findRef("
			  << op->getName() << ") finds op (bad)"
			  << endl;
		return false;
	} else if (checkOperationSignature(op->getName(), op->getParmList()) ) {
		kdDebug() << "UMLClassifier::addOperation: checkOperationSignature("
			  << op->getName() << ") op is non-unique" << endl;
		return false;
	}

	if( op -> parent() )
		op -> parent() -> removeChild( op );
	this -> insertChild( op );
	if( position >= 0 && position <= (int)m_OpsList.count() )
		m_OpsList.insert(position,op);
	else
		m_OpsList.append( op );
	emit childObjectAdded(op);
	emit operationAdded(op);
	emit modified();
	connect(op,SIGNAL(modified()),this,SIGNAL(modified()));
	return true;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLClassifier::addOperation(UMLOperation* Op, IDChangeLog* Log) {
	if( addOperation( Op, -1 ) )
		return true;
	else if( Log ) {
		Log->removeChangeByNewID( Op -> getID() );
	}
	return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
int UMLClassifier::removeOperation(UMLOperation *op) {
	if(!m_OpsList.remove(op)) {
		kdDebug() << "can't find opp given in list" << endl;
		return -1;
	}
	// disconnection needed.
	// note that we dont delete the operation, just remove it from the Classifier
	disconnect(op,SIGNAL(modified()),this,SIGNAL(modified()));
	emit childObjectRemoved(op);
	emit operationRemoved(op);
	emit modified();
	return m_OpsList.count();
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLOperation* UMLClassifier::takeOperation(UMLOperation* o) {
	if (removeOperation(o) >= 0) {
		return o;
	}
	return 0;
}

UMLObjectList UMLClassifier::findChildObject(UMLObject_Type t , QString n,
					     bool seekStereo /* = false */) {
	UMLObjectList list;
	if (t == ot_Association) {
		return UMLCanvasObject::findChildObject(t, n);
	} else if (t == ot_Operation) {
		UMLClassifierListItem* obj=0;
		for(obj=m_OpsList.first();obj != 0;obj=m_OpsList.next()) {
			if (obj->getBaseType() != t)
				continue;
			if (seekStereo) {
				if (obj->getStereotype() == n)
					list.append( obj );
			} else if (obj->getName() == n)
				list.append( obj );
		}
	} else {
		kdWarning() << "finding child object of unknown type: " << t << endl;
	}

	return list;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLObject* UMLClassifier::findChildObject(int id) {
	UMLClassifierListItem * o=0;
	for(o=m_OpsList.first();o != 0;o=m_OpsList.next()) {
		if(o->getID() == id)
			return o;
	}
	return UMLCanvasObject::findChildObject(id);
}

UMLObject* UMLClassifier::findChildObjectByIdStr(QString idStr) {
	UMLClassifierListItem *o = NULL;
	for (o = m_OpsList.first(); o; o = m_OpsList.next()) {
		if (o->getAuxId() == idStr)
			return o;
	}
	return NULL;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSubClassConcepts (ClassifierType /*type*/) {
  UMLAssociationList list = this->getGeneralizations();
	UMLClassifierList inheritingConcepts;
	int myID = this->getID();
  for (UMLAssociation *a = list.first(); a; a = list.next())
  {
    // Concepts on the "A" side inherit FROM this class
    // as long as the ID of the role A class isnt US (in
    // that case, the generalization describes how we inherit
    // from another class).
    // SO check for roleA id, it DOESNT match this concepts ID,
    // then its a concept which inherits from us
		if (a->getRoleId(A) != myID)
		{
			UMLObject* obj = a->getObject(A);
			UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
			if (concept)
				inheritingConcepts.append(concept);
		}

	}
	return inheritingConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSuperClassConcepts (ClassifierType /*type*/) {
  UMLAssociationList list = this->getGeneralizations();
	UMLClassifierList parentConcepts;
	int myID = this->getID();
  for (UMLAssociation *a = list.first(); a; a = list.next()) {
    // Concepts on the "A" side inherit FROM this class
    // as long as the ID of the role A class isnt US (in
    // that case, the generalization describes how we inherit
    // from another class).
    // SO check for roleA id, it DOESNT match this concepts ID,
    // then its a concept which inherits from us
		if (a->getRoleId(B) != myID) {
			UMLObject* obj = a->getObject(B);
			UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
			if (concept)
				parentConcepts.append(concept);
		}
	}
	return parentConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSuperInterfaceConcepts () {
  UMLClassifierList parentConcepts;
  int myID = this->getID();
  // First the realizations, ie the actual interface implementations
  UMLAssociationList list = this->getRealizations();
  for (UMLAssociation *a = list.first(); a; a = list.next()) {
    // Concepts on the "B" side are parent (super) classes of this one
    // So check for roleB id, it DOESNT match this concepts ID,
    // then its a concept which we inherit from
    if (a->getRoleId(B) != myID) {
      UMLObject* obj = a->getObject(B);
      UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
      if (concept)
        parentConcepts.append(concept);
    }
  }
  return parentConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findAllSuperClassConcepts () {
  UMLClassifierList result = findSuperClassConcepts();
  UMLClassifierList inters = findSuperInterfaceConcepts();
  for (UMLClassifier *a = inters.first();
       0 != a;
       a = inters.next()) {
    result.append(a);
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSuperAbstractConcepts () {
  int myID = this->getID();
  // First the interfaces
  UMLClassifierList parentConcepts = findSuperInterfaceConcepts();
  // Then add the generalizations of abstract objects
  UMLAssociationList list = this->getGeneralizations();
  for (UMLAssociation *a = list.first(); a; a = list.next()) {
    // Concepts on the "B" side are parent (super) classes of this one
    // So check for roleB id, it DOESNT match this concepts ID,
    // then its a concept which we inherit from
    if (a->getRoleId(B) != myID) {
      UMLObject* obj = a->getObject(B);
      UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
      if (concept && concept->getAbstract())
        parentConcepts.append(concept);
    }
  }
  return parentConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findImplementedAbstractConcepts () {
  // first get the list of interfaces we inherit from
  UMLClassifierList superint = findSuperAbstractConcepts();
  // now recursively find which interfaces we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = a->findImplementedAbstractConcepts();
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findAllImplementedAbstractConcepts () {
  // first get the list of objects we inherit from
  UMLClassifierList superint = findAllSuperClassConcepts();
  // now recursively find which interfaces we implement
  UMLClassifierList result;
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    if (a->getAbstract()) result.append(a);
    UMLClassifierList grandParents = a->findImplementedAbstractConcepts();
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findImplementedClasses () {
  // first get the list of classes we inherit from
  UMLClassifierList superint = findSuperClassConcepts();
  // now recursively find which classes we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = a->findImplementedClasses();
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findAllImplementedClasses () {
  // first get the list of classes we inherit from
  UMLClassifierList superint = findAllSuperClassConcepts();
  // now recursively find which classes we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = a->findAllImplementedClasses();
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLClassifier::operator==( UMLClassifier & rhs ) {
	if ( m_OpsList.count() != rhs.m_OpsList.count() ) {
		return false;
	}
	if ( &m_OpsList != &(rhs.m_OpsList) ) {
		return false;
	}
	return UMLCanvasObject::operator==(rhs);
}


void UMLClassifier::copyInto(UMLClassifier *rhs) const
{
	UMLCanvasObject::copyInto(rhs);

	m_OpsList.copyInto(&(rhs->m_OpsList));
}


////////////////////////////////////////////////////////////////////////////////////////////////////
// this perhaps should be in UMLClass/UMLInterface classes instead.
bool UMLClassifier::acceptAssociationType(Uml::Association_Type type)
{
	switch(type)
	{
		case at_Generalization:
		case at_Aggregation:
		case at_Dependency:
		case at_Association:
		case at_Association_Self:
		case at_Containment:
		case at_Composition:
		case at_Realization:
		case at_UniAssociation:
	 		return true;
		default:
			return false;
	}
	return false; //shutup compiler warning
}

bool UMLClassifier::hasAbstractOps () {
	UMLOperationList opl(getFilteredOperationsList());
	for(UMLOperation *op = opl.first(); op ; op = opl.next())
		if(op->getAbstract())
			return true;
	return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
int UMLClassifier::operations() {
	return m_OpsList.count();
}

////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierListItemList UMLClassifier::getOpList(bool includeInherited) {
	UMLClassifierListItemList ops(m_OpsList);
	if (includeInherited) {
		UMLClassifierList parents(findSuperClassConcepts());
		for (UMLClassifierListIt pit(parents); pit.current(); ++pit) {
			// get operations for each parent by recursive call
			UMLClassifierListItemList pops = pit.current()->getOpList(includeInherited);
			// add these operations to operation list, but only if unique.
			for (UMLClassifierListItem *po = pops.first(); po; po = pops.next()) {
				UMLClassifierListItem* o = ops.first();
				QString po_as_string(po->toString(Uml::st_SigNoScope));
				for (;o && o->toString(Uml::st_SigNoScope) != po_as_string ;o = ops.next())
					;
				if (!o)
					ops.append(po);
			}
		}
	}
	return ops;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
QPtrList<UMLOperation>*
UMLClassifier::getFilteredOperationsList(Scope permitScope,
                                         bool keepAbstractOnly)  {
	QPtrList<UMLOperation>* operationList = new QPtrList<UMLOperation>;
	for(UMLClassifierListItem* listItem = m_OpsList.first(); listItem;
	    listItem = m_OpsList.next())  {
		if (listItem->getBaseType() == ot_Operation) {
      UMLOperation* op = static_cast<UMLOperation*>(listItem);
      if (op->getScope() == permitScope &&
          (!keepAbstractOnly || op->getAbstract())) {
        operationList->append(op);
      }
		}
	}
	return operationList;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLOperationList UMLClassifier::getFilteredOperationsList(bool includeInherited)  {
	UMLClassifierListItemList classifierList(getOpList(includeInherited));
	UMLOperationList operationList;
	for(UMLClassifierListItem* listItem = classifierList.first(); listItem;
	    listItem = classifierList.next())  {
		if (listItem->getBaseType() == ot_Operation) {
			operationList.append(static_cast<UMLOperation*>(listItem));
		}
	}
	return operationList;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
void UMLClassifier::init() {
	m_BaseType = ot_UMLObject;
	m_OpsList.setAutoDelete(false);

	// make connections so that parent document is updated of list of uml objects

/* CHECK: Can we remove this code:
#ifdef __GNUC__
#warning "Cheap add/removeOperation fix for slot add/RemoveUMLObject calls. Need long-term solution"
#endif
	UMLDoc * parent = UMLApp::app()->getDocument();
	connect(this,SIGNAL(childObjectAdded(UMLObject *)),parent,SLOT(addUMLObject(UMLObject*)));
	connect(this,SIGNAL(childObjectRemoved(UMLObject *)),parent,SLOT(slotRemoveUMLObject(UMLObject*)));
 */
}

bool UMLClassifier::load(QDomElement& element) {
	QDomNode node = element.firstChild();
	element = node.toElement();
	while( !element.isNull() ) {
		QString tag = element.tagName();
		if (tagEq(tag, "Classifier.feature") ||
		    tagEq(tag, "Namespace.ownedElement") ||
		    tagEq(tag, "Namespace.contents")) {
			//CHECK: Umbrello currently assumes that nested elements
			// are features/ownedElements anyway.
			// Therefore these tags are not further interpreted.
			if (! load(element))
				return false;
		} else if (tagEq(tag, "Operation")) {
			UMLOperation* op = new UMLOperation(NULL);
			if (!op->loadFromXMI(element)) {
				kdError() << "UMLClassifier::load: error from op->loadFromXMI()"
					  << endl;
				delete op;
				return false;
			} else if (!this->addOperation(op) ) {
				kdError() << "UMLClassifier::load: error from this->addOperation(op)"
					  << endl;
				//return false;
				// Returning false here will spoil the entire
				// load. At this point the user has been warned
				// that something went wrong so let's still try
				// our best effort.
			}
		} else if (!loadSpecialized(element)) {
			UMLDoc *umldoc = UMLApp::app()->getDocument();
			UMLObject *pObject = umldoc->makeNewUMLObject(tag);
			if( !pObject ) {
				kdWarning() << "UMLClassifier::load: "
					    << "Unknown type of umlobject to create: "
					    << tag << endl;
				node = node.nextSibling();
				element = node.toElement();
				continue;
			}
			pObject->setUMLPackage(this);
			if (pObject->loadFromXMI(element)) {
				addObject(pObject);
				if (tagEq(tag, "Generalization"))
					umldoc->addAssocToConcepts((UMLAssociation *) pObject);
			} else {
				delete pObject;
			}
		}
		node = node.nextSibling();
		element = node.toElement();
	}//end while
	return true;
}

bool UMLClassifier::loadSpecialized(QDomElement& ) {
	// The UMLClass will override this for reading UMLAttributes.
	return true;
}

#include "classifier.moc"
