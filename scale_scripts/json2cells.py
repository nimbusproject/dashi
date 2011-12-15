import sys
import simplejson as json

def main(argv):

    file = argv[1]
    cols = argv[2:]

    f = open(file, "r")

    for l in f.readlines():
        j = json.loads(l.strip())

        for c in cols:
            if c in j:
                sys.stdout.write(str(j[c]))
            sys.stdout.write("|")
        print ""


if __name__ == '__main__':
    rc = main(sys.argv)
    sys.exit(rc)