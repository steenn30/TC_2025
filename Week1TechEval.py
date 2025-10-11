
myList = [1 , 2, 2, 3, 4, 5]
myDictionary = {}

for i in myList:
    print(i)
    entry = myList[i-1]

    if myDictionary.__contains__(myList[i-1]):
        myList.remove(i)
    else:
        myDictionary[myList[i-1]] = 1

print(myList)