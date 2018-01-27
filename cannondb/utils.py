"""
  This include some help-functions or classes.
"""
import random


def generate_address(kwargs: dict):
    """
    generate a address dynamically according to the length of kwargs..
    :return: (unsigned long long int)
    """
    if len(kwargs) < 0xFFFF:
        address = random.randint(0, 0xFFFF)
        while address in kwargs.keys():
            address = random.randint(0, 0xFFFF)
    elif len(kwargs) < 0xFFFFFFFF:
        address = random.randint(0xFFFF, 0xFFFFFFFF)
        while address in kwargs.keys():
            address = random.randint(0xFFFF, 0xFFFFFFFF)
    else:
        address = random.randint(0xFFFFFFFF, 0xFFFFFFFFFFFF - 1)
        while address in kwargs.keys():
            address = random.randint(0xFFFFFFFF, 0xFFFFFFFFFFFF - 1)
    return address
