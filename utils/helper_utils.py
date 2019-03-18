



# Checks if two dictionaries are equal
# TODO optimize this
def dict_equal(d1, d2):
    """ return True if all keys and values are the same """
    flag1= True
    flag2= True
    for key in d1:
        if not (key in d2 and d1[key] == d2[key]):
            flag1 = False
    for key in d2:
        if not (key in d1 and d1[key] == d2[key]):
            flag2 = False
    return flag1 and flag2

