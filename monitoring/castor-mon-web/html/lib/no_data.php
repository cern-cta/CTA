<?php
//This function create an image displaying the "No Avail. Data" Message 
function No_Data_Image() {
$data = "No Avail. Data ";
$img = imagecreatetruecolor(200,25);
$bgcol = imagecolorallocate($img,255,255,255);
imagefill($img,0,0,$bgcol);
imagestring($img,10,12,4,$data,NULL);
imagesetthickness($img,2);
imagerectangle($img,0,0,150,24,NULL);
header('Content-type: image/png');
imagepng($img);
}
?>