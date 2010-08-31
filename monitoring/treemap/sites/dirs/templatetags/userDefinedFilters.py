from django import template
register = template.Library()
from django.utils.html import conditional_escape
from django.utils.safestring import mark_safe


#addutional useful filters for the django template language

def mult(value, arg):
    "Multiplies the arg and the value"
    return int(value) * int(arg)
mult.is_safe = False

def sub(value, arg):
    "Subtracts the arg from the value"
    return int(value) - int(arg)
sub.is_safe = False

def div(value, arg):
    "Divides the value by the arg"
    return int(value) / int(arg)
div.is_safe = False

#def notzero(value, arg):
#    "Divides the value by the arg"
#    return int(value) / int(arg)
#div.is_safe = False

def equals(value, arg):
    return value == arg
equals.is_safe = True

def escapefilter(text, autoescape = None):
    if autoescape:
        esc = conditional_escape
    else:
        esc = lambda x: x
    result = text
    return mark_safe(result)
escapefilter.needs_autoescape = True



register.filter('mult', mult)
register.filter('sub', sub)
register.filter('div', div)
register.filter('escapefilter', escapefilter)
register.filter('equals', equals)
