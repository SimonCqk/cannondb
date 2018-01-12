'''
  This include some help-functions or classes.
'''
import random


def generate_address(kwargs: dict):
    '''
    generate a address dynamically according to the length of kwargs..
    :return: (unsigned long long int)
    '''
    if len(kwargs) < 0xFFFF:
        address = random.randint(0, 0xFFFF)
        while address in kwargs.keys():
            address = random.randint(0, 0xFFFF)
    elif len(kwargs) < 0xFFFFFFFF:
        address = random.randint(0xFFFF, 0xFFFFFFFF)
        while address in kwargs.keys():
            address = random.randint(0xFFFF, 0xFFFFFFFF)
    else:
        address = random.randint(0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF - 1)
        while address in kwargs.keys():
            address = random.randint(0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF - 1)
    return address


# Source:
# https://github.com/PythonCharmers/python-future/blob/466bfb2dfa36d865285dc31fe2b0c0a53ff0f181/future/utils/__init__.py#L102-L134


def with_metaclass(meta, *bases):
    """
    Function from jinja2/_compat.py. License: BSD.

    Use it like this::

        class BaseForm(object):
            pass

        class FormType(type):
            pass

        class Form(with_metaclass(FormType, BaseForm)):
            pass

    This requires a bit of explanation: the basic idea is to make a
    dummy metaclass for one level of class instantiation that replaces
    itself with the actual metaclass.  Because of internal type checks
    we also need to make sure that we downgrade the custom metaclass
    for one level to something closer to type (that's why __call__ and
    __init__ comes back from type etc.).

    This has the advantage over six.with_metaclass of not introducing
    dummy classes into the final MRO.
    """

    class Metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)

    return Metaclass('temporary_class', None, {})
