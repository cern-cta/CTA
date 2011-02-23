var IE = document.all?true:false;
if (!IE) 
	document.captureEvents(Event.MOUSEMOVE);

document.onmousemove = getMouseXY;

var mouseX = 0;
var mouseY = 0;

function getMouseXY(e) 
{
	if (IE) 
	{ // grab the x-y pos.s if browser is IE
		mouseX = event.clientX + document.body.scrollLeft;
		mouseY = event.clientY + document.body.scrollTop;
	}
	else 
	{  // grab the x-y pos.s if browser is NS
	mouseX = e.pageX;
	mouseY = e.pageY;
	}  
	
	if (mouseX < 0){mouseX = 0;}
	if (mouseY < 0){mouseY = 0;}  
	
	//document.coordinates.x.value = mouseX;
	//document.coordinates.y.value = mouseY;
	return true;
}

function getMouseX()
{
	return mouseX;
}

function getMouseY()
{
	return mouseY;
}