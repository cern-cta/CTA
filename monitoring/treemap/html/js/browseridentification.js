function isFirefox2plus()
{
	return window.globalStorage;
}

function isFirefox3plus()
{
	return window.globalStorage && window.postMessage;
}

function isOpera()
{
	return window.opera;
}

function isIE()
{
	var IE = /*@cc_on!@*/false;
	return IE || ('\v'=='v');
}

function IEVersion()
{
	//the order here is critical, don't change it!
	if(XDomainRequest) return 8;
	if(document.documentElement && typeof document.documentElement.style.maxHeight!="undefined") return 7;
	if(document.compatMode && document.all) return 6;
	if(window.createPopup) return 5.5;
	if(window.attachEvent) return 5;
	if(document.all) return 4;
	return 0;
}

function isSafari()
{
	return (/a/.__proto__=='//') && (window.devicePixelRatio);
}

function isChrome()
{
	return Boolean(window.chrome);

}

function detectedBrowser()
{
	if (isIE()) {return "IE "+IEVersion();}
	if (isFirefox3plus()) {return "Firefox 3+";}
	if (isFirefox2plus()) {return "Firefox 2+";}
	if (isOpera()) {return "Opera";}
	if (isSafari()) {return "Safari";}
	if (isChrome()) {return "Chrome";}
	return "not supported";
}