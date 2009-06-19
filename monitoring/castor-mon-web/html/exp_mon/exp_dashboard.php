<html>
  <head>
    <script type="text/javascript">
		
		/***********************************************
		* Image w/ description tooltip- By Dynamic Web Coding (www.dyn-web.com)
		* Copyright 2002-2007 by Sharon Paine
		* Visit Dynamic Drive at http://www.dynamicdrive.com/ for full source code
		***********************************************/
		
		/* IMPORTANT: Put script after tooltip div or 
			put tooltip div just before </BODY>. */
		
		var dom = (document.getElementById) ? true : false;
		var ns5 = (!document.all && dom || window.opera) ? true: false;
		var ie5 = ((navigator.userAgent.indexOf("MSIE")>-1) && dom) ? true : false;
		var ie4 = (document.all && !dom) ? true : false;
		var nodyn = (!ns5 && !ie4 && !ie5 && !dom) ? true : false;
		
		var origWidth, origHeight;
		
		// avoid error of passing event object in older browsers
		if (nodyn) { event = "nope" }
		
		///////////////////////  CUSTOMIZE HERE   ////////////////////
		// settings for tooltip 
		// Do you want tip to move when mouse moves over link?
		var tipFollowMouse= true;	
		// Be sure to set tipWidth wide enough for widest image
		var tipWidth= 500;
		var offX= 20;	// how far from mouse to show tip
		var offY= 12; 
		var tipFontFamily= "Verdana, arial, helvetica, sans-serif";
		var tipFontSize= "8pt";
		// set default text color and background color for tooltip here
		// individual tooltips can have their own (set in messages arrays)
		// but don't have to
		var tipFontColor= "#000000";
		var tipBgColor= "lightyellow"; 
		var tipBorderColor= "#000080";
		var tipBorderWidth= 0;
		var tipBorderStyle= "ridge";
		var tipPadding= 0;
		
		// to layout image and text, 2-row table, image centered in top cell
		// these go in var tip in doTooltip function
		// startStr goes before image, midStr goes between image and text
		var startStr = '<table width="' + tipWidth + '"><tr><td align="center" width="100%"><img src="';
		var midStr = '" border="0"></td></tr><tr><td valign="top">';
		var endStr = '</td></tr></table>';
		
		////////////////////////////////////////////////////////////
		//  initTip	- initialization for tooltip.
		//		Global variables for tooltip. 
		//		Set styles
		//		Set up mousemove capture if tipFollowMouse set true.
		////////////////////////////////////////////////////////////
		var pic = new Array ();
		function loadImage(imgsrc,i) {
			if (document.images) {
				pic[i] = new Image(500,300);
				pic[i].src = imgsrc;
			}
		}		
		
		var tooltip, tipcss;
		function initTip() {
			if (nodyn) return;
			tooltip = (ie4)? document.all['tipDiv']: (ie5||ns5)? document.getElementById('tipDiv'): null;
			tipcss = tooltip.style;
			if (ie4||ie5||ns5) {	// ns4 would lose all this on rewrites
				tipcss.width = tipWidth+"px";
				tipcss.fontFamily = tipFontFamily;
				tipcss.fontSize = tipFontSize;
				tipcss.color = tipFontColor;
				tipcss.backgroundColor = tipBgColor;
				tipcss.borderColor = tipBorderColor;
				tipcss.borderWidth = tipBorderWidth+"px";
				tipcss.padding = tipPadding+"px";
				tipcss.borderStyle = tipBorderStyle;
			}
			if (tooltip&&tipFollowMouse) {
				document.onmousemove = trackMouse;
			}
		}
		
		window.onload = initTip;
		
		/////////////////////////////////////////////////
		//  doTooltip function
		//			Assembles content for tooltip and writes 
		//			it to tipDiv
		/////////////////////////////////////////////////
		var t1,t2;	// for setTimeouts
		var tipOn = false;	// check if over tooltip link
		function doTooltip(evt,imgstr) {
			if (!tooltip) return;
			if (t1) clearTimeout(t1);	if (t2) clearTimeout(t2);
			tipOn = true;
			// set colors if included in messages array
			curBgColor = tipBgColor;
			curFontColor = tipFontColor;
			if (ie4||ie5||ns5) {
				var tip = startStr + imgstr + midStr + '<span style="font-family:' + tipFontFamily + '; font-size:' + tipFontSize + '; color:' + curFontColor + ';">' + '</span>' + endStr;
				tipcss.backgroundColor = curBgColor;
				tooltip.innerHTML = tip;
			}
			if (!tipFollowMouse) positionTip(evt);
			else t1=setTimeout("tipcss.visibility='visible'",100);
		}
		
		
		var mouseX, mouseY;
		function trackMouse(evt) {
			standardbody=(document.compatMode=="CSS1Compat")? document.documentElement : document.body //create reference to common "body" across doctypes
			mouseX = (ns5)? evt.pageX: window.event.clientX + standardbody.scrollLeft;
			mouseY = (ns5)? evt.pageY: window.event.clientY + standardbody.scrollTop;
			if (tipOn) positionTip(evt);
		}
		
		/////////////////////////////////////////////////////////////
		//  positionTip function
		//		If tipFollowMouse set false, so trackMouse function
		//		not being used, get position of mouseover event.
		//		Calculations use mouseover event position, 
		//		offset amounts and tooltip width to position
		//		tooltip within window.
		/////////////////////////////////////////////////////////////
		function positionTip(evt) {
			if (!tipFollowMouse) {
				standardbody=(document.compatMode=="CSS1Compat")? document.documentElement : document.body
				mouseX = (ns5)? evt.pageX: window.event.clientX + standardbody.scrollLeft;
				mouseY = (ns5)? evt.pageY: window.event.clientY + standardbody.scrollTop;
			}
			// tooltip width and height
			var tpWd = (ie4||ie5)? tooltip.clientWidth: tooltip.offsetWidth;
			var tpHt = (ie4||ie5)? tooltip.clientHeight: tooltip.offsetHeight;
			// document area in view (subtract scrollbar width for ns)
			var winWd = (ns5)? window.innerWidth-20+window.pageXOffset: standardbody.clientWidth+standardbody.scrollLeft;
			var winHt = (ns5)? window.innerHeight-20+window.pageYOffset: standardbody.clientHeight+standardbody.scrollTop;
			// check mouse position against tip and window dimensions
			// and position the tooltip 
			if ((mouseX+offX+tpWd)>winWd) 
				tipcss.left = mouseX-(tpWd+offX)+"px";
			else tipcss.left = mouseX+offX+"px";
			if ((mouseY+offY+tpHt)>winHt) 
				tipcss.top = winHt-(tpHt+offY)+"px";
			else tipcss.top = mouseY+offY+"px";
			if (!tipFollowMouse) t1=setTimeout("tipcss.visibility='visible'",100);
		}
		
		function hideTip() {
			if (!tooltip) return;
			t2=setTimeout("tipcss.visibility='hidden'",100);
			tipOn = false;
		}
		
		document.write('<div id="tipDiv" style="position:absolute; visibility:hidden; z-index:100"></div>')
		
    </script>
    <title> Experiments Monitor
    </title>
    <style>
		#fonts {
			font: 12px/15px arial, helvetica, sans-serif;
		}
    </style>	 
  </head>
<?php
//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
$service = $_GET['service'];
error_reporting(E_ALL ^ E_NOTICE);
echo "
  <META HTTP-EQUIV='Refresh' CONTENT='300; URL=exp_dashboard.php?service=$service'> 
  </head>
  <body>
    <table bgcolor = 'lightyellow'>
      <tr>
        <td align = left>";include("exp_read.php");
echo "  </td>
        </tr>
	<tr>
        <td align = left>";include("exp_write.php");
echo "</tr>
    </table>
  </body>
</html>";
?>










