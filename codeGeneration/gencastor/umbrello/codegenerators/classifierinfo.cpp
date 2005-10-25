
/***************************************************************************
			  classifierinfo.cpp  -  description
			     -------------------
    copyright	    : (C) 2003 Brian Thomas brian.thomas@gsfc.nasa.gov
***************************************************************************/

/***************************************************************************
 *									 *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.				   *
 *									 *
 ***************************************************************************/


#include "../classifier.h"
#include "classifierinfo.h"

#include "../class.h"
#include "../interface.h"
#include "../operation.h"

ClassifierInfo::ClassifierInfo( UMLClassifier *classifier, UMLDoc *doc)
{
	init(classifier, doc);
}

ClassifierInfo::~ClassifierInfo() { }

UMLClassifierList ClassifierInfo::findSuperClassConcepts
(UMLClassifier *c) {
  UMLAssociationList list = c->getSpecificAssocs(Uml::at_Generalization);
	UMLClassifierList parentConcepts;
	int myID = c->getID();
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

UMLClassifierList ClassifierInfo::findSuperInterfaceConcepts
(UMLClassifier *c) {
  UMLClassifierList parentConcepts;
  int myID = c->getID();
  // First the realizations, ie the actual interface implementations
  UMLAssociationList list = c->getRealizations();
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

UMLClassifierList ClassifierInfo::findAllSuperClassConcepts
(UMLClassifier *c) {
  UMLClassifierList result = findSuperClassConcepts(c);
  UMLClassifierList inters = findSuperInterfaceConcepts(c);
  for (UMLClassifier *a = inters.first();
       0 != a;
       a = inters.next()) {
    result.append(a);
  }
  return result;
}

UMLClassifierList ClassifierInfo::findAllImplementedClasses
(UMLClassifier *c) {
  // first get the list of classes we inherit from
  UMLClassifierList superint = findAllSuperClassConcepts(c);
  // now recursively find which classes we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = findAllImplementedClasses(a);
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}

UMLClassifierList ClassifierInfo::findSuperAbstractConcepts
(UMLClassifier *c) {
  int myID = c->getID();
  // First the interfaces
  UMLClassifierList parentConcepts = findSuperInterfaceConcepts(c);
  // Then add the generalizations of abstract objects
  UMLAssociationList list = c->getSpecificAssocs(Uml::at_Generalization);
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

UMLClassifierList ClassifierInfo::findImplementedAbstractConcepts
(UMLClassifier *c) {
  // first get the list of interfaces we inherit from
  UMLClassifierList superint = findSuperAbstractConcepts(c);
  // now recursively find which interfaces we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = findImplementedAbstractConcepts(a);
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}

UMLClassifierList ClassifierInfo::findAllImplementedAbstractConcepts
(UMLClassifier *c) {
  // first get the list of objects we inherit from
  UMLClassifierList superint = findAllSuperClassConcepts(c);
  // now recursively find which interfaces we implement
  UMLClassifierList result;
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    if (a->getAbstract()) result.append(a);
    UMLClassifierList grandParents = findImplementedAbstractConcepts(a);
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}

UMLOperationList*
ClassifierInfo::getFilteredOperationsList
(UMLClassifier *c, Scope permitScope, bool keepAbstractOnly) {
  UMLOperationList baseList = c->getFilteredOperationsList(false);
  QPtrList<UMLOperation>* operationList = new QPtrList<UMLOperation>;
	for(UMLOperation* listItem = baseList.first(); listItem;
	    listItem = baseList.next())  {
    if (listItem->getScope() == permitScope &&
        (!keepAbstractOnly || listItem->getAbstract())) {
      operationList->append(listItem);
    }
	}
	return operationList;
}

void ClassifierInfo::init(UMLClassifier *c, UMLDoc */*doc*/) {

	// make all QPtrLists autoDelete false
	atpub.setAutoDelete(false);
	atprot.setAutoDelete(false);
	atpriv.setAutoDelete(false);

	static_atpub.setAutoDelete(false);
	static_atprot.setAutoDelete(false);
	static_atpriv.setAutoDelete(false);

	// set default class, file names
	packageName = c->getPackage();
  fullPackageName = packageName;
  fullPackageName.replace(".", "::");
  if (!fullPackageName.isEmpty()) fullPackageName.append("::");
	className = c->getName();
	fileName = c->getName().lower();

	// determine up-front what we are dealing with
	if(dynamic_cast<UMLInterface*>(c))
		isInterface = true;
	else
		isInterface = false;
  isAbstract = c->getAbstract();

	// set id
	m_nID = c->getID();

	// sort attributes by Scope
	if(!isInterface) {
		UMLClass * myClass = dynamic_cast<UMLClass *>(c);
		UMLAttributeList *atl = myClass->getFilteredAttributeList();
		for(UMLAttribute *at=atl->first(); at ; at=atl->next()) {
			switch(at->getScope())
			{
			case Uml::Public:
				if(at->getStatic())
					static_atpub.append(at);
				else
					atpub.append(at);
				break;
			case Uml::Protected:
				if(at->getStatic())
					static_atprot.append(at);
				else
					atprot.append(at);
				break;
			case Uml::Private:
				if(at->getStatic())
					static_atprot.append(at);
				else
					atpriv.append(at);
				break;
			}
			m_AttsList.append(at);
		}
	}

	// inheritance issues
	superclasses = c->getSuperClasses(); // list of what we inherit from
	superclasses.setAutoDelete(false);

  // list of all classes we inherit from (recursively built)
	allSuperclasses = findAllImplementedClasses(c);
	allSuperclasses.setAutoDelete(false);
  for (UMLClassifier *uc = allSuperclasses.first();
       0 != uc;
       uc = allSuperclasses.next()) {
    allSuperclassIds[uc->getID()] = true;
  }

  superInterfaces = findSuperInterfaceConcepts(c); // list of interfaces  we inherit from
  superInterfaces.setAutoDelete(false);

  superAbstracts = findSuperAbstractConcepts(c); // list of interfaces/abstract classes  we inherit from
  superAbstracts.setAutoDelete(false);

  implementedAbstracts = findImplementedAbstractConcepts(c); // list of interfaces/abstract classes we directly implement
  implementedAbstracts.setAutoDelete(false);

  allImplementedAbstracts =
    findAllImplementedAbstractConcepts(c); // list of interfaces/abstract classes we implement
  allImplementedAbstracts.setAutoDelete(false);

	subclasses = c->getSubClasses();     // list of what inherits from us
	subclasses.setAutoDelete(false);

	// another preparation, determine what we have
	plainAssociations = c->getSpecificAssocs(Uml::at_Association); // BAD! only way to get "general" associations.
  UMLAssociationList ul =
    c->getSpecificAssocs(Uml::at_UniAssociation);
  for (unsigned int i = 0; i < ul.count(); i++) {
    plainAssociations.append(ul.at(i));
  }
  UMLAssociationList dl =
    c->getSpecificAssocs(Uml::at_Dependency);
  for (unsigned int i = 0; i < dl.count(); i++) {
    plainAssociations.append(dl.at(i));
  }
	plainAssociations.setAutoDelete(false);

	aggregations = c->getAggregations();
	aggregations.setAutoDelete(false);

	compositions = c->getCompositions();
	compositions.setAutoDelete(false);

	generalizations = c->getSpecificAssocs(Uml::at_Generalization);
	generalizations.setAutoDelete(false);

	realizations = c->getRealizations();
	realizations.setAutoDelete(false);

  // List of all attributes
  allAttributes = m_AttsList;
  for (UMLClassifier *uc = allSuperclasses.first();
       0 != uc;
       uc = allSuperclasses.next()) {
    UMLClass * myClass = dynamic_cast<UMLClass *>(uc);
    if (0 == myClass) continue;
    UMLAttributeList *atl = myClass->getFilteredAttributeList();
    if (0 == atl) continue;
    for(UMLAttribute *at=atl->first(); at ; at=atl->next()) {
      allAttributes.append(at);
    }
  }
      
	// set some summary information about the classifier now
	hasAssociations = plainAssociations.count() > 0 || aggregations.count() > 0 || compositions.count() > 0;
  hasAttributes[Uml::Public] = false;
  hasAttributes[Uml::Protected] = false;
	hasAttributes[Uml::Private] =
    atpriv.count() > 0 || static_atpriv.count() > 0 ||
    atprot.count() > 0 || static_atprot.count() > 0 ||
    atpub.count() > 0 || static_atpub.count() > 0;

	hasStaticAttributes[Uml::Public] = static_atpub.count() > 0;
  hasStaticAttributes[Uml::Protected] = static_atprot.count() > 0;
  hasStaticAttributes[Uml::Private] = static_atpriv.count() > 0;

	hasAccessorMethods[Uml::Public] =
    atpub.count() > 0 || static_atpub.count() > 0 || hasAssociations;
	hasAccessorMethods[Uml::Protected] =
    atprot.count() > 0 || static_atprot.count() > 0;
	hasAccessorMethods[Uml::Private] =
    atpriv.count() > 0 || static_atpriv.count() > 0;

  UMLOperationList opl = c->getFilteredOperationsList();
	hasOperationMethods[Uml::Public] = false;
	hasOperationMethods[Uml::Protected] = false;
	hasOperationMethods[Uml::Private] = false;
  for(UMLClassifierListItem* item = opl.first(); item; item = opl.next()) {
    hasOperationMethods[item->getScope()] = true;
	}
  if (!c->getAbstract()) {
    for (UMLClassifier *interface = implementedAbstracts.first();
         interface !=0;
         interface = implementedAbstracts.next()) { 
      opl = interface->getFilteredOperationsList();
      for(UMLClassifierListItem* item = opl.first(); item; item = opl.next()) {
        if (item->getAbstract()) {
          hasOperationMethods[item->getScope()] = true;
        }
      }
    }
  }

	hasMethods[Uml::Public] =
    hasOperationMethods[Uml::Public] || hasAccessorMethods[Uml::Public];
	hasMethods[Uml::Protected] =
    hasOperationMethods[Uml::Protected] || hasAccessorMethods[Uml::Protected];
	hasMethods[Uml::Private] =
    hasOperationMethods[Uml::Private] || hasAccessorMethods[Uml::Private];

	// this is a bit too simplistic..some associations are for
	// SINGLE objects, and WONT be declared as Vectors, so this
	// is a bit overly inclusive (I guess that's better than the other way around)
	hasVectorFields = hasAssociations ? true : false;
}

UMLClassifierList ClassifierInfo::getPlainAssocChildClassifierList() {
	return findAssocClassifierObjsInRoles(&plainAssociations);
}

UMLClassifierList ClassifierInfo::getAggregateChildClassifierList() {
	return findAssocClassifierObjsInRoles(&aggregations);
}

UMLClassifierList ClassifierInfo::getCompositionChildClassifierList() {
	return findAssocClassifierObjsInRoles(&compositions);
}

UMLClassifierList ClassifierInfo::findAssocClassifierObjsInRoles (UMLAssociationList * list)
{


	UMLClassifierList classifiers;
	classifiers.setAutoDelete(false);

	for (UMLAssociation *a = list->first(); a; a = list->next()) {
		// DONT accept a classfier IF the association role is empty, by
		// convention, that means to ignore the classfier on that end of
		// the association.
		// We also ignore classfiers which are the same as the current one
		// (e.g. id matches), we only want the "other" classfiers
		if (a->getRoleId(A) == m_nID && a->getRoleName(B) != "") {
			UMLClassifier *c = dynamic_cast<UMLClassifier*>(a->getObject(B));
			if(c)
				classifiers.append(c);
		} else if (a->getRoleId(B) == m_nID && a->getRoleName(A) != "") {
			UMLClassifier *c = dynamic_cast<UMLClassifier*>(a->getObject(A));
			if(c)
				classifiers.append(c);
		}
	}

	return classifiers;
}

UMLAttributeList* ClassifierInfo::getAttList() {
	        return &m_AttsList;
}

