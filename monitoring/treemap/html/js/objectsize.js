function Size(x, y, w, h)
{
    this.x = x;
    this.y = y;
    this.w = w;
    this.h = h;
}
        
function getObjectSize(n)
{
    var s = new Size(n.offsetLeft, n.offsetTop, n.offsetWidth, n.offsetHeight);
    while (n.offsetParent && n.offsetParent.offsetLeft !== undefined)
    {
        n = n.offsetParent;
        s.x += n.offsetLeft;
        s.y += n.offsetTop;
    }
    if (isFirefox2plus()){s.y = s.y-60;}
    return s;
}