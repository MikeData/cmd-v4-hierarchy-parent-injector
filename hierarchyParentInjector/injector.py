
import pandas as pd

# Config
class config(object):

    def __init__(self):
        self.abortOnIssues = True

        # Warn the user if they're living dangerously
        if not self.abortOnIssues:
            print(
                "/n WARNING !!/n. Aborting on encountering an issue is turned off via config.py. This should NEVER be turned off without " +
                + "good reason as this is your principle defence againt data pollution.")


# Object to hold simple errors we can use later
class simpleErrors(object):
    cfg = config()

    # specified codelist does not exist. Always errors.
    def codeListName(self, codeList, dataset):
        raise ValueError("The codelist name you have specified '{cl}' is not present in the column headers of the provided dataset '{ds}'.".format(cl=codeList, ds=dataset))

    # specified time dim does not exist. Always errors.
    def noTime(self, timeDim):
        raise ValueError("Aborting. Cannot find a time dimension of '{t}'. Please use the time= keyword to specify when time dimension is not called 'time'.".format(t=timeDim))

    # specified geography codelist does not exist. Always errors.
    def noGeography(self, geographyDim):
        raise ValueError("Aborting. Cannot find a geography codelist dimension of '{g}'. Please use the geography= keyword to specify when time dimension is not called 'geography_codelist'.".format(g=geographyDim))

    # has obs level data. we cant handle that. Always errors
    def obsLevelData(self):
        raiseValueError("Aborting. Source data has obs level data (pre-time columns). We cannot create values for these.")

    # cannot find a label associated with a given child code within the hierarchy file. Always errors.
    def cantFindLabel(self, code):
        raise ValueError("Cannot find a label with the hierarchy file to represent the code: " + str(code))

    # cannot find an expected observation for a combination of dimensions
    def cantFindObs(self, row):
        raise ValueError("Aborting. Unable to find observation to populate the following (sub)total row: " ",".join(row))

    # parent nodes already have data. Errors by defaut but can just warn.
    def parentWithData(self):
        print("WARNING - populated parent nodes detected!")
        print("Outputting rows (with index) as 'ERRORS_popParents.csv'.")
        if cfg.abortOnIssues:
            raise ValueError("Aborting: The data contains populated parent nodes")

    # error for if we hit an endless loop during extraction. Always errors.
    def abortInfiniteExtractionLoop(self, parentChildDict):
        text = "\n\n Aborting: unable to continue extraction. While populating the parent nodes, we've encountered a scenario where not all parents have been populated," + \
        " no parent has enough populated children to be computed and no further children-that-are-parents can be computed. \n\nUnextracted parent:children are:\n\n " + str(parentChildDict)
        raise ValueError(text)




# #####################
# Our principle class
class injectParents(object):

    # We'll need a hierarchy dataframe, plus the codelist name and all dimension item codes
    def __init__(self, hierINfile, dataINfile, codeList, time="time", geography="geography_codelist", populatePrimeNode=False):

        def loadDataFrame(frame):
            df = pd.read_csv(frame)
            df.fillna("", inplace=True)
            return df

        # Names and dataframes for source files
        self.hierINname = hierINfile
        self.dataINname = dataINfile
        self.dfH = loadDataFrame(hierINfile)      # dataframe for hierarchy csv
        self.dfD = loadDataFrame(dataINfile)      # dataframe for data/v4 csv

        # do we want to total the whole tree on the top node?
        self.populatePrimeNode = populatePrimeNode

        # Specified column names
        self.codeList = codeList
        self.timeDim = time
        self.geographyCodelist = geography

        # neater error handling
        self.simpleErrors = simpleErrors()

        # Early abort if there's an issue
        self.sanityChecks()

        # Derived vars we don't want to keep calculating
        self.lowestLevelCodes = self.getLowestLevelCodes()
        self.combinationFrame = self.allOtherCombinationsFrame()

        # EXTRACT
        self.populateParents()


    # Confirm that we've been provided the correct data
    def sanityChecks(self):

        # Do we have the spcified codelist in the dataset?
        if self.codeList not in self.dfD.columns.values:

            # If they've just forgotten the _codelist postfix then just fix it
            if self.codeList + "_codelist" in self.dfD.columns.values:
                self.codeList = self.codeList + "_codelist"
            else:
                self.simpleErrors.codeListName(self.codeList, self.dataINname)

        # Do we have the specified time dimension in the dataset?
        if self.timeDim not in self.dfD.columns.values:
            self.simpleErrors.noTime(self.timeDim)

        # Do we have the specified geography dimension in the dataset?
        if self.geographyCodelist not in self.dfD.columns.values:
                self.simpleErrors.noGeography(self.geographyCodelist)

        # error is there is obs level metadata
        if "V4_0" not in self.dfD.columns.values:
            self.errors.obsLevelData()

        # Make damn sure there's not already some totals in there
        self.parseForExistingTotals()


    # Confirms that NO DATA is provided for any node that has a child.
    def parseForExistingTotals(self):

        # Get a list of all parent codelists from the hierarchy.csv
        allParents = [x for x in self.dfH["ParentCode"].unique() if x != ""]

        # Create a dataframe of rows from the v4 that contain codes that are parents. i.e are already (sub)totals
        parents = self.dfD[self.dfD[self.codeList].map(lambda x: x in allParents)]

        # If that parent dataframe has rows in it, output as csv and throw an error/warning
        if len(parents) > 0:
            parents.to_csv("ERRORS_popParents.csv")
            self.simpleErrors.parentWithData()


    # List of all codes that are not themselves parents
    def getLowestLevelCodes(self):

        codes = list(self.dfH["Code"].unique())
        parents = list(self.dfH["ParentCode"].unique())

        return [x for x in codes if x not in parents]


    # makes a dictionary of {parent:[child1, child2, child3]}
    def makeParentchildrenDict(self):

        # the "prime node" is the top of the tree so its the one row where ParentCode is blank
        if self.populatePrimeNode:
            allParents = [x for x in self.dfH["ParentCode"].unique()]
        else:
            allParents = [x for x in self.dfH["ParentCode"].unique() if x != ""]

        cpDict = {}
        for parent in allParents:

            childrenOfParent = list(self.dfH["Code"][self.dfH["ParentCode"] == parent].unique())
            cpDict.update({parent:childrenOfParent})

        return cpDict


    # generate a dtaframe with one of all possible dimension combinations that must exist for any code in our chosen codelist
    # leaves V4_0, X_codelist and X blank.
    def allOtherCombinationsFrame(self):

        # new dataframe to hack down
        df = self.dfD.copy()

        # now blank code and label for our hierachied dimension as well as the v4_0 col
        codeCol = self.codeList
        labelIndex = self.dfD.columns.get_loc(self.codeList) + 1
        labelCol = self.dfD.columns.values[labelIndex]

        # now blank code, label and v4_0 col
        df["V4_0"] = ""
        df[codeCol] = ""
        df[labelCol] = ""

        # drop duplicates
        df = df.drop_duplicates()

        # reset the index
        df = df.reset_index(drop=True)

        return df



    # given a parent code and its child codes. Populate the dataframe with (sub) totals.
    def makeTotal(self, parent, children):

        def getLabelFromCode(code):

            # for every code - get the corresponding label from the hierarchy file
            labelFrame = self.dfH[self.dfH["Code"] == parent]
            allLabels = list(labelFrame["Label"].unique())

            # throw an error if not exactly one return variable in list
            if len(allLabels) != 1:
                self.simpleErrors.cantFindLabel(parent)
            label = allLabels[0]

            return label

        # Now populate the codelist and associated labels for our parent
        codeCol = self.codeList
        labelCol = self.dfD.columns.get_loc(self.codeList) + 1


        # ####################
        # GET THE OBS TO TOTAL
        # ####################
        # Iterator...this will be slow

        # results of parsing
        # creates a dict of
        # {
        #  rowNumber : {
        #               child1: obsforChild1
        #               child2: obsforChild2
        #               child3: obsForChild3
        # }
        rowChildObsDict = {}

        for child in children:

            tempFrame = self.combinationFrame.copy()

            tempFrame[codeCol] = child
            tempFrame[tempFrame.columns.values[labelCol]] = getLabelFromCode(child)

            for rowIndex, row in tempFrame.iterrows():

                outFrame = self.dfD

                for i in range(1, len(row)-1):

                    col = self.dfD.columns.values[i]
                    outFrame = outFrame[outFrame[col] == row[col]].copy()

                if len(outFrame) != 1:
                    self.simpleErrors.cantFindObs(row)
                else:
                    ob = outFrame["V4_0"].unique()[0]

                # update results dict
                if rowIndex not in rowChildObsDict.keys():
                    rowChildObsDict.update({rowIndex:{}})

                rowChildObsDict[rowIndex].update({child:ob})


        # #########
        # TOTAL OBS
        # #########

        # We're going to create a clean "all combinations" frame and inject the totals from the
        # child values held in rowChildObsDict

        obsFrame = self.combinationFrame.copy()

        # populate the codelist in question
        obsFrame[self.codeList] = parent

        # populate the label for the codelist
        label = getLabelFromCode(parent)
        obsFrame[obsFrame.columns.values[labelCol]] = label

        for rowIndex, row in obsFrame.iterrows():

            childValuesForRow = rowChildObsDict[rowIndex]
            childValuesList = [childValuesForRow[x] for x in childValuesForRow.keys()]

            obTotal = sum(childValuesList)

            obsFrame.at[rowIndex, 'V4_0'] = obTotal


        # Merge into original dataframe then return it
        self.dfD = pd.concat([self.dfD, obsFrame])

        return self.dfD


    # Selects a single node from the parent:children dict, sends for processing then returns
    def processOneNode(self, parentChildDict):

        abort = True  # until we find a node we can process

        # Start by populating parents that have lowest level children first
        for parent in parentChildDict.keys():

            children = parentChildDict[parent]
            foundChildren = [x for x in children if x in self.lowestLevelCodes]

            # If all specified children are lowest level children (so will not require sub-totalling first)
            # them can total here as a total of "whatever children are present in the dataset"
            # this is required as not all lowest level hierarchy items appear in each dataset that implements that hierarchy
            if len(children) == len(foundChildren):
                self.dfD = self.makeTotal(parent, children)

                # done, delete key and return
                del parentChildDict[parent]
                abort = False
                break

        # if abort is still True, we're out of parents with lowest level children so need to build totals and sub-totals using the heirarchy as a reference
        if abort:

            # if we don't have all the expected child nodes for a given parent we'll pass on it until we do.
            # the total here is explicitly a total of "ALL children for parent"
            # if we never have all the required children to process a parents then so be it - we return abort=True and an infiniteExtractionLoopError will get raised.

            for parent in parentChildDict.keys():

                children = parentChildDict[parent]

                # get codes from current (as in, its been updated since we started) source dataframe
                currentCodes = self.dfD[self.codeList].unique()
                foundChildren = [x for x in children if x in currentCodes]
                if len(children) == len(foundChildren):
                    self.dfD = self.makeTotal(parent, children)

                    # done, delete key and return
                    del parentChildDict[parent]
                    abort = False
                    break

        return parentChildDict, abort


    def populateParents(self):

        # Get a list of all parent codelists from the hierarchy.csv
        parentChildDict = self.makeParentchildrenDict()

        # Some of our parents will themselves be totals of other parents.
        # so we need to do multiple passes, only totaling when all children for a
        # given parent have been added to the dataset.

        while len(parentChildDict) > 0:

            parentChildDict, abort = self.processOneNode(parentChildDict)

            if abort:
                self.simpleErrors.abortInfiniteExtractionLoop(parentChildDict)


        self.dfD.to_csv("ParentsInjected_" + self.dataINname, index=False)
