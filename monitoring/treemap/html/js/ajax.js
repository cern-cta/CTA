//this is not used, it's just to keep potentially useful code
function getAjaxObject()
{
	req = null;
	try
	{
	    req = new XMLHttpRequest();
	}
	catch (e)
	{
	   try
	   {
	        req = new ActiveXObject("Msxml2.XMLHTTP");
	   } 
	   catch (e)
	   {
	      try
	     {
	          req = new ActiveXObject("Microsoft.XMLHTTP");
	     } 
	     catch (failed)
	     {
	         req = null;
	     }
	  }  
	}	  
	return req;
}

/*
function updateStatus()
{
	  ajax = getAjaxObject();
	  if (ajax == null) alert("no ajax object found!");
	  ajax.onreadystatechange = function()
	  {       
		  //alert(ajax.responseText);
		  //alert(ajax + ajax.readyState + "_" + ajax.status + "_" + ajax.responseText + this);
	      if(ajax.readyState == 4) 
	      {
			alert(ajax.status);
	        if(ajax.status == 200) 
		    {
	            alert(ajax.responseText);
	            //document.getElementById("thesubtable").style.display = "none";
				return true;
	        }
	        else
		    {    
				return false;
	        }
	      }
	      else 
		  {
			  return false;
		  }
	  };

	ajax.open("GET", "{{relstatuspath}}", true);
	ajax.send(null); 
}*/

//