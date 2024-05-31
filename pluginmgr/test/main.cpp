#include <iostream>

#include "PluginLoader.hpp"

class IMathFunction
{
public:
	virtual const char* Name() const = 0;
	virtual double  Eval(double x) const = 0;	
	virtual ~IMathFunction() = default;
};


int main()
{
	PluginManager ma;
	IPluginFactory* infoA = ma.addPlugin("PluginA");
	// * infoA = ma.GetPluginInfo("PluginA");
	assert(infoA != nullptr);
	
	std::cout << " ---- Plugin Information --------- " << "\n"
			  << "  =>              Name = " << infoA->Name() << "\n"
			  << "  =>           Version = " << infoA->Version() << "\n"
			  << "  => Number of classes = " << infoA->NumberOfClasses()
			  << "\n\n";

	std::cout << "Classes exported by the Plugin: (PluginA) " << "\n";

	for(size_t n = 0; n < infoA->NumberOfClasses(); n++)
	{
		std::cout << " -> Exported Class: " << infoA->GetClassName(n) << "\n";
	}
	
	
	// Type of pExp: std::shared_ptr<IMathFunction>
	auto pExp = ma.CreateInstanceAs<IMathFunction>("PluginA", "Exp");
	assert(pExp != nullptr);
	
	std::cout << "pExp->Name()    = " << pExp->Name() << "\n";
	std::cout << "pExp->Eval(3.0) = " << pExp->Eval(3.0) << "\n";
	
	auto pLog = ma.CreateInstanceAs<IMathFunction>("PluginA", "Log");
	std::cout << "pLog->Name()    = " << pLog->Name() << "\n";
	std::cout << "pLog->Eval(3.0) = " << pLog->Eval(3.0) << "\n";
	
	return 0;
}
