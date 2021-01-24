from copy import copy
import re, os


class catalog:
    """ Read ASCII catalogs """

    def __init__(self):
        self.filename = ""
        self.data = {}  # keys are number
        self.header = {}  # Header{index} = keyname
        self.header_ntok = {}  # Header_ntok{index} = number of rows
        self.ncol = 0
        self.rkeys = []  # indexes
        self.revheader = {}
        self.lkeys = []

    def get_lkeys(self):
        return self.lkeys

    def set_filename(self, filename0):
        self.filename = filename0

    def get_nkey(self):
        return len(self.rkeys)

    def get_nobj(self):
        # print self.header
        # print self.data
        return len(self.data[self.rkeys[0]])

    def dump_header(self):
        head_string = ""
        indd = 0
        for ind in self.rkeys:
            head_string += "# " + str(ind) + " " + self.header[str(ind)] + '\n'
            indd += 1
        return (indd, head_string)

    def reverse_dict(self):
        self.revheader = dict([(x, y) for y, x in self.header.iteritems()])

    # Read the whole catalog
    # if long : collapses the keyword name
    def read_cat(self, sep="", long=False):
        if self.filename == "":
            return 1

        try:
            file = open(self.filename, 'r')
        except IOError:
            print "Impossible to open " + self.filename
            return 1

        lines = file.readlines()
        file.close()

        # Get the keywords
        if long == False:
            pattern = r'#\s+(\d+)\s+(\S+)'
        else:
            pattern = r'#\s+(\d+)\s+(.+)'
        regex = re.compile(pattern)
        line = ""
        for line in lines:
            match = regex.search(line)
            if match:
                if long == False:
                    par = match.group(2)
                else:
                    par = "".join(match.group(2).split())
                self.header[match.group(1)] = par
                self.ncol += 1
                self.data[match.group(1)] = []
                self.lkeys.append(par)

        # print "TEST"
        # for k,v in self.header.items():
        #	print k,v
        # sys.exit(0)
        # print self.ncol

        # Get the data
        for line in lines:
            if line[0] != "#":
                if sep == "":
                    list = line.split()
                else:
                    list = line.split(sep)
                if len(list) != self.ncol:
                    print line
                    print "Huge problem ... Bad number of columns ..."
                    # print list
                    print len(list), self.ncol
                    return 1
                for key in self.header:
                    self.data[str(key)].append(list[int(key) - 1])

        # print "test2"

        # store the read keys
        for kk in self.header:
            # print kk
            self.rkeys.append(kk)

        self.rkeys.sort(lambda x, y: cmp(int(x), int(y)))

        # print self.header
        # sys.exit(0)

        return 0

    # Read a list of keyword indexes
    def read_keys_num(self, keys):
        if self.filename == "":
            return 1

        try:
            file = open(self.filename, 'r')
        except IOError:
            print "Impossible to open " + self.filename
            return 1

        lines = file.readlines()
        file.close()

        # Get the keywords
        pattern = r'#\s+(\d+)\s+(\S+)'
        regex = re.compile(pattern)
        for line in lines:
            match = regex.search(line)
            if match:
                self.header[match.group(1)] = match.group(2)
                self.ncol += 1
                self.data[match.group(1)] = []

        # Get the data
        for line in lines:
            if line[0] != "#":
                list = line.split()
                if len(list) != self.ncol:
                    return 1
                for key in keys:
                    self.data[str(key)].append(list[int(key) - 1])

        # Store the keys
        for k in keys:
            self.rkeys.append(k)

        # Check if a key exists in the catalog

    def has_key(self, key):
        for k in self.header:
            if key == self.header[k]:
                return 1
        return 0

    # Read a list of keywords
    def read_keys(self, keys):
        if self.filename == "":
            return 1

        try:
            file = open(self.filename, 'r')
        except IOError:
            print "Impossible to open " + self.filename
            return 2

        lines = file.readlines()
        file.close()

        # Get the keywords
        pattern = r'#\s+(\d+)\s+(\S+)'
        regex = re.compile(pattern)
        for line in lines:
            match = regex.search(line)
            if match:
                self.header[match.group(1)] = match.group(2)
                self.ncol += 1
                self.data[match.group(1)] = []

        # Convert key_num to key
        keys_name = []
        b = dict([(x, y) for y, x in self.header.iteritems()])
        for k in keys:
            if b.has_key(k):
                keys_name.append(b[k])

        # Get the data
        # print self.ncol
        for line in lines:
            if line[0] != "#":
                list = line.split()
                if len(list) != self.ncol:
                    print len(list), self.ncol
                    return 3
                for key in keys_name:
                    self.data[str(key)].append(list[int(key) - 1])

        # Store the keys
        for k in keys:
            self.rkeys.append(b[k])

        return 0

    # Get data (single or list / numbers or keyword names)
    def get_data_number(self, key_num):
        if not str(key_num) in self.rkeys:
            return 1
        return self.data[str(key_num)]

    # Get 1 data (single or list / numbers or keyword names)
    def get_1data_number(self, key_num, ind):
        if not str(key_num) in self.rkeys:
            return 1
        return self.data[str(key_num)][ind]

    # Get data (single or list / numbers or keyword names)
    def get_1data_numberf(self, key_num, ind):
        if not str(key_num) in self.rkeys:
            return 1
        return float(self.data[str(key_num)][ind])

    def get_data_key(self, key):
        b = dict([(x, y) for y, x in self.header.iteritems()])
        if not b.has_key(key):
            print 'Error ... not ' + key + ' keyword in ' + str(b)
            return 1
        key_num = b[key]
        # print 'key_num=',key_num
        # print self.rkeys
        if not key_num in self.rkeys:
            return 2
        return self.data[key_num]

    def get_1data_key(self, key, ind):
        if not self.revheader.has_key(key):
            self.reverse_dict()
        if not self.revheader.has_key(key):
            print 'Error ... not ' + key + ' keyword in ' + str(b)
            return 1
        key_num = self.revheader[key]
        # print 'key_num=',key_num
        # print self.rkeys
        if not key_num in self.rkeys:
            return 2
        return self.data[key_num][ind]

    def get_1data_keyf(self, key, ind):
        if not self.revheader.has_key(key):
            self.reverse_dict()
        if not self.revheader.has_key(key):
            print 'Error ... not ' + key + ' keyword in ' + str(b)
            return 1
        key_num = self.revheader[key]
        # print 'key_num=',key_num
        # print self.rkeys
        if not key_num in self.rkeys:
            return 2
        return float(self.data[key_num][ind])

    def get_data_keyn(self, key):
        b = dict([(x, y) for y, x in self.header.iteritems()])
        if not b.has_key(key):
            return 1
        key_num = b[key]
        # print 'key_num=',key_num
        # print self.rkeys
        if not key_num in self.rkeys:
            return 2
        list = []
        for v in self.data[key_num]:
            list.append(float(v))
        return list

    def get_data_key_flt(self, key):
        b = dict([(x, y) for y, x in self.header.iteritems()])
        if not b.has_key(key):
            return 1
        key_num = b[key]
        if not key_num in self.rkeys:
            return 1
        y = 0
        return [float(y) for y in self.data[key_num]]

    def get_data_numbers(self, keys):
        data = {}
        for k in keys:
            if not str(k) in self.rkeys:
                continue
            d = self.get_data_number(k)
            data[k] = copy(d)
        return data

    def get_data_keys(self, keys):
        data = {}
        b = dict([(x, y) for y, x in self.header.iteritems()])
        for k in keys:
            if b.has_key(k):
                key_num = b[k]
                d = self.get_data_number(key_num)
                data[k] = copy(d)
        return data

    def print_header(self, filename):
        try:
            file = open(filename, 'w')
        except IOerror:
            print "Impossible to open " + filename
            return 1
        ind = 1
        for k in self.rkeys:
            file.write('#\t' + str(ind) + '\t' + self.header[k] + '\n')
            ind += 1
        file.close()

    def print_line(self, index, flow):
        for k in self.rkeys:
            flow.write(str(self.data[k][index]) + '\t')
        flow.write('\n')

    def dump_line(self, index):
        dump_str = ""
        for k in self.rkeys:
            dump_str += str(self.data[k][index]) + '\t'
        return dump_str

    def print_data(self, filename):
        try:
            file = open(filename, 'a')
        except IOerror:
            print "Impossible to open " + filename
            return 1
        k1 = self.data.items()[0][0]
        ndata = len(self.data[k1])
        for i in range(ndata):
            self.print_line(i, file)
        file.close()

    def create_cat(self, data, list_head):
        """Create a catalog from a dictionnary and a list of headers"""

        # Header
        for k, ind in zip(list_head, range(1, len(list_head) + 1, 1)):
            # print k,ind
            self.header[ind] = k
            self.ncol += 1
            self.data[ind] = []
            self.rkeys.append(ind)

        print data

        # Data
        ndata = len(data[list_head[-1]])
        for ind in range(ndata):
            for k, v in self.header.items():
                self.data[k].append(data[v][ind])

    def check_same(self, data, ind):
        """ check if data already in the catalog """
        ndata_in = self.get_ndata()
        for i in range(ndata_in):
            flag = 0
            for k, v in self.header.items():
                if str(self.data[k][i]) != str(data[v][ind]):
                    flag = 1
                    break
            if flag == 0:
                return 1
        return 0

    def add_data(self, data):
        # check data for all keywords
        for k in self.header.values():
            if not k in data.keys():
                print 'Big problem ... keyword ' + k + ' missing'
                return 1

        # add data
        ndata = len(data[data.items()[0][0]])
        for ind in range(ndata):
            if not self.check_same(data, ind):
                for k, v in self.header.items():
                    self.data[k].append(data[v][ind])

        return 0

    def get_ndata(self):
        return len(self.data[self.header.items()[0][0]])


# *******************************

def convert_to_ASCII(incat, outcat):
    """ Check if catalog is ASCII or ldac and convert to ASCII """

    res = os.popen("file " + incat)
    lines = res.readlines()
    tmpcat = ""
    if lines[0].find("FITS") != -1:
        print "ldactoasc " + incat + " > " + outcat
        os.system("ldactoasc " + incat + " > " + outcat)
    else:
        print "cp " + incat + "  " + outcat
        os.system("cp " + incat + "  " + outcat)
