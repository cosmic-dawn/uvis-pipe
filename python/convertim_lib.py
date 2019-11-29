import astropy.io.fits as pyfits


class conv_im:
    """ Convert image (like ESO -> CFHT) """

    def __init__(self, filename0, instrument0=""):
        self.filename = filename0
        self.instrument = instrument0  # Instrument
        self.format = instrument0  # Header format
        self.next = 0  # Number of extentions
        self.pyim = None  # Pyfits structure
        self.user_tabconv_filename = ""

        # Default conversion tables for various instruments
        self.conv_tables = {}

        self.conv_tables["VISTA"] = {}
        self.conv_tables["VISTA"]["WIRCAM"] = {}

        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"] = ["AIRMASS", "FILTER"]

        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"] = {}
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["UTC"] = "UT"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["LST"] = "ST"
#        # self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["RA"] = "RA"
#        # self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["DEC"] = "DEC"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO TEL AIRM START"] = "AIRMASS"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO DPR TYPE"] = "IMAGETYP"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS FILT1 NAME"] = "FILTER"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS FILT2 NAME"] = "FILTER2"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS FILT3 NAME"] = "FILTER3"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS FILT4 NAME"] = "FILTER4"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS SLIT2 NAME"] = "SLIT"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRIS1 NAME"] = "GRISM"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRAT NAME"] = "GRAT"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRAT1 NAME"] = "GRAT1"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRAT2 NAME"] = "GRAT2"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRAT WLEN"] = "WLEN"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRAT1 WLEN"] = "WLEN1"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRAT2 WLEN"] = "WLEN2"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS GRAT ORDER"] = "ORDER"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO OBS PROG ID"] = "RUNID"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["RA"] = "RA_DEG"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["DEC"] = "DEC_DEG"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["MJD-OBS"] = "MJDATE"

#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["ARCFILE"] = "FILENAME"
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO TEL ECS FLATFIELD"] = "IMRED_FF"

        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"] = {}
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["CTYPE1"] = "RA---TAN"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["CTYPE2"] = "DEC--TAN"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["RADECSYS"] = "FK5"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["EQUINOX"] = 2000.0
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["SATURATE"] = 30000.0

        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_DELETE"] = []
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_DELETE"] = ["PROJP1", "PROJP3", "PV2_1", "PV2_2", "PV2_3",  "PV2_4", "PV2_5"]

        # DEBUGGING
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["DETECTOR"] = "WIRCam"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["INSTRUME"] = "WIRCam"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["TELESCOP"] = "CFHT 3.6m"
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"].append("RUNID")
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"].append("RA_DEG")
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"].append("DEC_DEG")
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"].append("IMRED_FF")
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"].append("IMRED_MK")
        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"].append("OBJECT")

        #-----------------------------------------------------------------------------
        # VISTA_FLAT
        #-----------------------------------------------------------------------------

        self.conv_tables["VISTA_FLAT"] = {}
        self.conv_tables["VISTA_FLAT"]["WIRCAM"] = {}

        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_COPY"] = ["AIRMASS", "FILTER"]

        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"] = {}
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["UTC"] = "UT"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["LST"] = "ST"
        # self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["RA"] = "RA"
        # self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["DEC"] = "DEC"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO TEL AIRM START"] = "AIRMASS"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO DPR TYPE"] = "IMAGETYP"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO INS FILT1 NAME"] = "FILTER"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO OBS PROG ID"] = "RUNID"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["RA"] = "RA_DEG"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS"]["DEC"] = "DEC_DEG"
        # self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS"]["HIERARCH ESO TEL ECS FLATFIELD"] = "IMRED_FF"

        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"] = {}
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["CTYPE1"] = "RA---TAN"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["CTYPE2"] = "DEC--TAN"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["RADECSYS"] = "FK5"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["EQUINOX"] = 2000.0
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["DETECTOR"] = "WIRCAM"
        # self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_UPDATE"]["DETECTOR"] = "VISTA"

        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_DELETE"] = ["PROJP1", "PROJP3", "PV2_1", "PV2_2", "PV2_3",
                                                                       "PV2_4", "PV2_5"]

        # DEBUGGING
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["DETECTOR"] = "WIRCam"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["INSTRUME"] = "WIRCam"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_UPDATE"]["TELESCOP"] = "CFHT 3.6m"
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_COPY"].append("RUNID")
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_COPY"].append("RA_DEG")
        self.conv_tables["VISTA_FLAT"]["WIRCAM"]["KEYWORDS_COPY"].append("DEC_DEG")
#        self.conv_tables["VISTA"]["WIRCAM"]["KEYWORDS_COPY"].append("OBJECT")

    def print_tab(self, inn="USER", out="USER2"):
        if not self.conv_tables.has_key(inn):
            print "No Table for input instrument " + inn
            return 1
        if not self.conv_tables[inn].has_key(out):
            print "No Table for output instrument " + out
            return 1

        lkeys = self.conv_tables[inn][out]["KEYWORDS"].keys()
        lcopykeys = self.conv_tables[inn][out]["KEYWORDS_COPY"]
        ldelkeys = self.conv_tables[inn][out]["KEYWORDS_DELETE"]

        print "\nConvertion Table :\n\n"
        print "  - Keywords to copy :"
        for k in lcopykeys:
            print "      - " + k

        print "  - Keywords to transform :"
        for k in lkeys:
            print "      - " + k + " --> " + self.conv_tables[inn][out]["KEYWORDS"][k]

        print "  - Keywords to delete :"
        for k in ldelkeys:
            print "      - " + k

        return 0

    def read_tab_conv(self, filename, inst1="USER", inst2="USER2"):
        """ Read a table to convert the header keywords """

        self.user_tabconv_filename = filename

        # init
        self.conv_tables[inst1] = {}
        self.conv_tables[inst1][inst2] = {}
        self.conv_tables[inst1][inst2]["KEYWORDS_COPY"] = []
        self.conv_tables[inst1][inst2]["KEYWORDS"] = {}

        # Read in the file
        file = open(filename, 'r')
        lines = file.readlines()
        file.close()

        for line in lines:
            if len(line.strip()) > 0 and line.strip()[0] == "#":
                continue
            line0 = line.strip()
            if len(line.strip()) > 0 and line0.split()[0] == "COPY":
                list = line0.split()[1]
                for k in list:
                    self.conv_tables[inst1][inst2]["KEYWORDS_COPY"].append(k)
            elif len(line.strip()) > 0:
                list = line0.split("=")
                # print list
                if len(list) != 2:
                    continue
                else:
                    self.conv_tables[inst1][inst2]["KEYWORDS"][list[0]] = list[1]
        self.print_tab(inst1, inst2)

    def open_image(self):
        """ Open the image """
        try:
            self.pyim = pyfits.open(self.filename, do_not_scale_image_data=True)
        except:
            print "Error in opening image ... " + self.pyim

        # N extention
        if len(self.pyim) == 1:
            self.next = 1
        else:
            self.next = len(self.pyim) - 1

    def convert_im(self, inst1="USER", inst2="USER2", remove="no"):
        """ Convert image ... """

        # Check for table_conv
        if not self.conv_tables.has_key(inst1):
            print "No Table for input instrument " + inst1
            return 1
        if not self.conv_tables[inst1].has_key(inst2):
            print "No Table for output instrument " + inst2
            return 1

        # Image opened
        if self.pyim == None:
            print "You need to read to open the image first ..."
            return 1

        # Convert the keys of extention 0
        for key in self.conv_tables[inst1][inst2]['KEYWORDS']:

            # Update keys
            if key in self.pyim[0].header:
                self.pyim[0].header.update({self.conv_tables[inst1][inst2]['KEYWORDS'][key]: self.pyim[0].header[key]})
                if remove == "yes":
                    del self.pyim[0].header[key]
            else:
                print "Impossible to update (not found in ext0) key " + key
                # return 1
        # Update values of ext 0
        for key in self.conv_tables[inst1][inst2]['KEYWORDS_UPDATE']:
            self.pyim[0].header.update({key: self.conv_tables[inst1][inst2]['KEYWORDS_UPDATE'][key]})

        # Specific to VISTA (Flatfield)
        if inst1 == "VISTA" and inst2 == "WIRCAM":
            flatname = self.pyim[1].header["FLATCOR"].split("[")[0] + 's'
            self.pyim[0].header.update({'IMRED_FF': flatname})
        # Specific to VISTA (Bad pixel mask)
        if inst1 == "VISTA" and inst2 == "WIRCAM":
            bpmname = self.pyim[1].header["LINCOR"].split("[")[0].replace("chan", "bpm") + 's'
            self.pyim[0].header.update({'IMRED_MK': bpmname})

        # Delete some keys
        for key in self.conv_tables[inst1][inst2]['KEYWORDS_DELETE']:
            try:
                del self.pyim[0].header[key]
            except:
                print("Tried to remove non-existing key {}.".format(key))

        # Store the values to copy
        copykey = {}
        for key in self.conv_tables[inst1][inst2]['KEYWORDS_COPY']:
            if not key in self.pyim[0].header:
                print "Impossible to copy key " + key + " from extention 0."
                # return 1
            copykey[key] = self.pyim[0].header[key]

        # Do it for the other extentions
        for iext in range(self.next + 1)[1:]:
            # modify
            for key in self.conv_tables[inst1][inst2]['KEYWORDS']:
                if key in self.pyim[iext].header:
                    self.pyim[iext].header.update(
                        {self.conv_tables[inst1][inst2]['KEYWORDS'][key]: self.pyim[iext].header[key]})
                    if remove == "yes":
                        del self.pyim[iext].header[key]
                else:
                    print "Impossible to update (not found in ext " + str(iext) + ") key " + key
                    # return 1

            # Copy keys
            for key in self.conv_tables[inst1][inst2]['KEYWORDS_COPY']:
                self.pyim[iext].header.update({key: copykey[key]})

            # Delete keys
            for key in self.conv_tables[inst1][inst2]['KEYWORDS_DELETE']:
                if key in self.pyim[iext].header:
                    del self.pyim[iext].header[key]

            # Update values
            for key in self.conv_tables[inst1][inst2]['KEYWORDS_UPDATE']:
                self.pyim[iext].header.update({key: self.conv_tables[inst1][inst2]['KEYWORDS_UPDATE'][key]})

        # Put the instrument name in the header of the first extention
        self.pyim[0].header.update({"INST_0LD": inst1})
        self.pyim[0].header.update({"INST": inst2})

        return 0

    def close_file(self, outname):
        if self.pyim != None:
            self.pyim.writeto(outname, output_verify="fix")
            self.pyim.close()

    def destroy(self):
        """ Destructor """
        if self.pyim != None:
            self.pyim.close()

    def add_keys_fromStack(self, data_STkeys, data_stacks, list_keystacks, data_progenitors):
        """ Add keywords from the stack st files """

        im = self.filename.replace(".fits", "")

        # Get the right stack
        stack = ""
        for st, listim in data_progenitors.iteritems():
            if im in listim:
                stack = st
        if stack == "":
            print "Big problem ... stack not found for ", im
            sys.exit(1)

        # print im,stack

        # Get the right parameters
        params = data_stacks[stack]
        for i, k in enumerate(list_keystacks):
            pars = params[i]
            ikey = k
            okey = data_STkeys[ikey]
            if len(pars) == 1:  # Write in first extention
                self.pyim[0].header.update(okey, pars[0])
            elif len(pars) == 16:  # Write in all image extentions
                for iext in range(16):
                    self.pyim[iext + 1].header.update({okey: pars[iext]})
            elif len(pars) == 17:  # Write in all extentions
                for iext in range(17):
                    self.pyim[iext].header.update(okey, pars[iext])

        # Write stack name
        for iext in range(17):
            self.pyim[iext].header.update({"STACK": stack})

        return 0

    def add_keys_fromQCfiles(self, files, data_keys):
        """Add keywords from the QC files """

        return 0
