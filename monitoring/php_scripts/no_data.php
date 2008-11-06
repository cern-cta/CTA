/******************************************************************************
 *              no_data.php
 *
 * This file is part of the Castor Monitoring project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2008  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Author Theodoros Rekatsinas, 
 *****************************************************************************/
<?php 
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