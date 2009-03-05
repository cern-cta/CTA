<?php 
function No_Data_Image() {
$data = "";
$img = imagecreatetruecolor(1,1);
$bgcol = imagecolorallocate($img,0,0,0);
imagefill($img,0,0,$bgcol);
imagestring($img,0,0,0,$data,NULL);
imagesetthickness($img,0);
imagerectangle($img,0,0,0,0,NULL);
header('Content-type: image/png');
imagepng($img);
}
?>