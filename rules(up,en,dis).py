import fileinput, re


#input data from file
fin = open ("input", "r")
fin2 = fin.readline ().split ("\n")[0]
LKA = fin.readlines ()
command = LKA[0]
files = LKA[1]
fin.close ()

file = open (files, "r")
lis = file.read ()
file.close ()

file = open (files, "r")

sid_new = fin2.split ('sid:')[1].split (';')[0]

l = []

for line in file.readlines ():
    if len (line.split ('{}'.format (('sid:' + str (sid_new) + ';')))) > 1:
        l.append (line)


def update (l, fin2, files):
    if l == []:
        print ("Incorrect data update!")

    else:
        if l[0][0] == "#":
            fin2 = "# " + str (fin2)

        for lined in fileinput.input (files, inplace=1):
            print (lined.replace (l[0], fin2).split ("\n")[0])


def enable (l, fin2, files):
    if l == []:
        f = open ('local.rules', 'a')
        f.write (fin2)

    else:
        for lined in fileinput.input (files, inplace=1):
            l_1 = re.sub ('# ', '', l[0])
            print (lined.replace (l[0], l_1).split ("\n")[0])


def disable (l, files):
    if l == []:
        print ("Not data from this sid!")

    else:
        if l[0][0] == "#":
            pass
        else:
            for lined in fileinput.input (files, inplace=1):
                l_1 = re.sub ('^', '# ', l[0])
                print (lined.replace (l[0], l_1).split ("\n")[0])


# You have 3 commands: update, enable, disable. If not, get error


if command == 'update\n':
    update (l, fin2, files)
elif command == 'enable\n':
    enable (l, fin2, files)
elif command == 'disable\n':
    disable (l, files)
else:
    print ("Incorrect command!")
