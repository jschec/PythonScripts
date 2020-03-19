#ped_coverage_aggregator.py

import os
from os.path import join
import pandas as pd
from sqlite_client import *

#CONSTANTS
COVERAGE_FILE_ENDING = ".coverage_data.txt"
FILE_STORAGE_PATH = "/archive/dobyns_w/exome_data/all_exome_files"
pedigreeBasicData = "/active/mirzaa_g/Projects/Projects-Active/ML_WES/analysis_josh/tracking_sheets/master_ped_tracking_22120.xlsx"

class Directory:
    path = ""
    fileList = []
    directoryList = []

    def __init__(self, path):
        self.path = path
        self.searchDirectory()

    def searchDirectory(self):
        directoryList = []
        fileList = []

        for itemName in os.listdir(self.path):
            if os.path.isfile(os.path.join(self.path, itemName)):
                fileList.append(itemName)
            else:
                directoryList.append(itemName)

        self.directoryList = directoryList
        self.fileList = fileList

    def getFileList(self):
        return self.fileList

    def getDirectoryList(self):
        return self.directoryList

    def findFile(self, name):
        foundFile = "Not found"
        for fileName in self.fileList:
            if name in fileName:
                foundFile = self.path + "/" + fileName
                break
        return foundFile


    def getPath(self):
        return self.path

class Patient:
    identifier = ""
    directoryPath = ""

    def __init__(self, identifier, dataStorePath):
        self.identifier = identifier
        self.directoryPath = dataStorePath + "/" + identifier

    def getIdentifier(self):
        return self.identifier

    def getDirectoryPath(self):
        return self.directoryPath

class Coverage:
    filePath = ""
    df = None

    def __init__(self, filePath):
        self.filePath = filePath
        self.df = pd.read_table(filePath)

    def addColumnLabel(self, label):
        labelList = []
        for i in range(len(self.df)):
            labelList.append(label)
        self.df["family_id"] = labelList
        
    def getFilePath(self):
        return self.filePath
    
    def getDf(self):
        return self.df

class PatientCoverage:
    patient = None
    directory = None
    coverageDataFound = False
    coverageFilePath = ""
    coverage = None

    def __init__(self, patientIdentifier, patientWESFileIdentifier, dataStorePath):
        print(f"Retrieving {patientIdentifier} coverage.")
        self.patient = Patient(patientIdentifier, dataStorePath)
        try:
            self.directory = Directory(self.patient.getDirectoryPath())
            self.coverageFilePath = self.directory.findFile(COVERAGE_FILE_ENDING) 
            self.readCoverageData()
        except:
            print("Directory Not found")
        
        self.directory = None

    def readCoverageData(self):
        if self.directory == None or self.coverageFilePath == "Not found":
            print("Not found")
        else:
            self.coverage = Coverage(self.coverageFilePath)
            self.coverage.addColumnLabel(self.patient.getIdentifier())
            self.coverageDataFound = True
            print("Retrieved")
    
    def getCoverage(self):
        return self.coverage.getDf()

    def getCoverageDataFound(self):
        return self.coverageDataFound

class PedigreeList:
    patientList = []
    patientMissingCoverageList = []
    mappingDf = None
    coverageDf = pd.DataFrame()


    def __init__(self, mappingDf):
        self.mappingDf = mappingDf
        self.aggregateCoverageData()

    def aggregateCoverageData(self):
        for index, row in self.mappingDf.iterrows():
            patientCoverageRow = PatientCoverage(row["family_id"], row["family_wes_id"], FILE_STORAGE_PATH)
            if patientCoverageRow.getCoverageDataFound():
                self.coverageDf = pd.concat([self.coverageDf, patientCoverageRow.getCoverage()], ignore_index=True)
            else:
                self.patientMissingCoverageList.append(row["family_id"])
    def saveMissingPedigreesList(self, path):
        df = pd.DataFrame({"MissingPeds": self.patientMissingCoverageList})
        df.to_csv(path)

    def mergeDataFrames(self):
        print("Merging dataframes...")
        return pd.merge(self.mappingDf, self.coverageDf, on='family_id', how='left')


if __name__ == "__main__":


    dxCodesToInclude = ["DEVN", "MIC", "MEG", "MCD", "MHM", "Other"]
    diagnosisFilterStmt = ' OR '.join((f"`Dx Group 1` = '{diagnosis}'") for diagnosis in dxCodesToInclude)

    sqliteDb = SQLiteDB('/active/mirzaa_g/Projects/Projects-Active/ML_WES/analysis_josh/scripts/dbs/pedigreeCoverage.db')
    sqliteDb.import_file(pedigreeBasicData, "pedigree_t", "excel")
    pedigreeDf = sqliteDb.execute_select_stmt(f"SELECT family_id, family_wes_id, classification, `NIH Ethnicity`, Gender, `Dx Group 1` AS Dx1 \
        FROM pedigree_t WHERE {diagnosisFilterStmt}")
    pedigreeList = PedigreeList(pedigreeDf)
    pedigreeList.saveMissingPedigreesList("/active/mirzaa_g/Projects/Projects-Active/ML_WES/analysis_josh/scripts/coverageData/missingPedList.csv")
    outputDf = pedigreeList.mergeDataFrames()
    outputDf.to_csv('/active/mirzaa_g/Projects/Projects-Active/ML_WES/analysis_josh/scripts/coverageData/pedCoverageAggregated.csv')


    newOutputDf = outputDf
    print()
    print()
    print()

    for index, row in outputDf.iterrows():
        dadlabel = row["family_id"] + "f"
        momLabel = row["family_id"] + "m"
        if type(row["bam file"]) == float:
            continue
        if dadlabel in row["bam file"] or momLabel in row["bam file"]:
            print(f"Removed {row['bam file']}")
            newOutputDf = newOutputDf.drop(index=index)

    newOutputDf.to_csv('/active/mirzaa_g/Projects/Projects-Active/ML_WES/analysis_josh/scripts/coverageData/probandCoverageAggregated.csv')






