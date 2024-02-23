
l1 = [(1,'I love to play cricket'),(2,'I am into motorbiking'),(3,'What do you like')]

for e in l1:
    length = len(e)

    if (length == 1):
        print(" {} ".format(e[0]))
    elif (length == 2):
        print(" {} \t {}".format(e[0],e[1]))
    elif (length == 3):
        print(" {} \t {} \t {} ".format(e[0], e[1], e[2]))
    elif (length == 4):
        print(" {} \t {} \t {} \t {} ".format(e[0], e[1], e[2], e[3]))
    elif (length == 5):
        print(" {} \t {} \t {} \t {} \t {} ".format(e[0], e[1], e[2], e[3], e[4]))
