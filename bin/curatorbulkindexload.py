#
# Program: curatorbulkindexload.py
#
# Inputs:
#
#	A tab-delimited file in the format:
#	field 1: J:
#	field 2: MGI ID (allele, marker, strain)
#	field 3: Modified By
#
# Outputs:
#
#       1 BCP files:
#
#       MGI_Reference_Assoc
#
#       Diagnostics file of all input parameters and SQL commands
#       Error file
#
# History
#
# lec	09/06/2023
#	wts2-1248/Bulk index associations (allele, marker, strain)
#

import sys
import os
import db
import mgi_utils
import loadlib

#db.setTrace()

inputFileName = os.environ['INPUT_FILE_DEFAULT']
mode = ''
isSanityCheck = 0
lineNum = 0
hasFatalError = 0
hasWarningError = 0

diagFileName = os.environ['LOG_DIAG']
errorFileName = os.environ['LOG_ERROR']
inputFile = os.environ['INPUTDIR']
outputFile = os.environ['OUTPUTDIR']

diagFile = ''		# diagnostic file descriptor
errorFile = ''		# error file descriptor
assocFile = ''          # file descriptor

assocTable = 'MGI_Reference_Assoc'

assocFileName = assocTable + '.bcp'

assocKey = 0            # MGI_Reference_Assoc._Assoc_key

cdate = mgi_utils.date('%m/%d/%Y')	# current date
 
# Purpose: prints error message and exits
# Returns: nothing
# Assumes: nothing
# Effects: exits with exit status
# Throws: nothing
def exit(
    status,          # numeric exit status (integer)
    message = None   # exit message (string)
    ):

    if message is not None:
        sys.stderr.write('\n' + str(message) + '\n')
 
    try:
        diagFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))

        if hasFatalError == 0:
                errorFile.write("\nSanity check : successful\n")
        else:
                errorFile.write("\nSanity check : failed")
                errorFile.write("\nErrors must be fixed before file is published.\n")

        errorFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        diagFile.close()
        errorFile.close()
        inputFile.close()
    except:
        pass

    db.useOneConnection(0)
    sys.exit(status)
 
# Purpose: process command line options
# Returns: nothing
# Assumes: nothing
# Effects: initializes global variables
#          calls showUsage() if usage error
#          exits if files cannot be opened
# Throws: nothing
def init():
    global inputFileName, mode, isSanityCheck
    global diagFileName, errorFileName, diagFile, errorFile, inputFile
    global assocFile
 
    try:
        inputFileName = sys.argv[1]
        mode = sys.argv[2]
    except:
        exit(1, 'Could not open inputFileName=sys.argv[1] or mode=sys.argv[2]\n')

    if mode == "preview":
        isSanityCheck = 1

    # place diag/error file in current directory
    if isSanityCheck == 1:
        diagFileName = inputFileName + '.diagnostics'
        errorFileName = inputFileName + '.error'

    try:
        if isSanityCheck == 1:
            diagFile = open(diagFileName, 'w')
        else:
            diagFile = open(diagFileName, 'a')
    except:
        exit(1, 'Could not open file diagFile: %s\n' % diagFile)
                
    try:
        errorFile = open(errorFileName, 'w')
    except:
        exit(1, 'Could not open file errorFile: %s\n' % errorFile)
                
    try:
        inputFile = open(inputFileName, 'r', encoding="latin-1")
    except:
        exit(1, 'Could not open file inputFileName: %s\n' % inputFileName)
    
    if isSanityCheck == 0:
        try:
                assocFile = open(outputFile + '/' + assocFileName, 'w')
        except:
                exit(1, 'Could not open file %s\n' % assocFileName)

    # Log all SQL
    db.set_sqlLogFunction(db.sqlLogAll)

    diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    diagFile.write('Server: %s\n' % (db.get_sqlServer()))
    diagFile.write('Database: %s\n' % (db.get_sqlDatabase()))

    errorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

    return

# Purpose:  verify Object
# Returns:  objectKey, mgiTypeKey
# Assumes:  nothing
# Effects:  verifies that the Object exists as a primary object Allele (11), or Marker (2) or Strain (10)
#	writes to the error file if the Object is invalid
# Throws:  nothing
def verifyObject(
    mgiid, 	# MGI ID (string)
    lineNum	# line number (integer)
    ):

    global hasFatalError, hasWarningError

    objectKey = 0
    mgitypeKey = 0
    assocTypeKey = 0

    results = db.sql('''
    select _object_key, _mgitype_key from ACC_Accession where _mgitype_key in (2,10,11) and preferred = 1 and accid = \'%s\'
    ''' % (mgiid), 'auto')

    if len(results) == 0:
            errorFile.write('Invalid Object (row %d): %s\n' % (lineNum, mgiid))
            hasFatalError += 1
    elif len(results) > 1:
            errorFile.write('Object Returns > 1 result (row %d): %s\n' % (lineNum, mgiid))
            hasFatalError += 1
    else:
        mgitypeKey = r['_mgitype_key']
        if mgitypeKey not in (2,10,11):
                errorFile.write('Invalid Object MGI Type Key (row %d): %s, %s\n' % (lineNum, mgiid, mgitypekey))
                hasFatalError += 1
                mgitypeKey = 0
        else:
                objectKey = r['_object_key']
                if mgitypeKey == 2:
                        assocTypeKey = 1018
                elif mgitypeKey == 10:
                        assocTypeKey = 1031
                else:
                        assocTypeKey = 1013
                        
    return objectKey, mgitypeKey, assocTypeKey

# Purpose:  sets global primary key variables
# Returns:  nothing
# Assumes:  nothing
# Effects:  sets global primary key variables
# Throws:   nothing
def setPrimaryKeys():

    global asscoKey

    results = db.sql(''' select nextval('mgi_reference_assoc_seq') as maxKey ''', 'auto')
    assocKey = results[0]['maxKey']

# Purpose:  processes data
# Returns:  nothing
# Assumes:  nothing
# Effects:  verifies and processes each line in the input file
# Throws:   nothing
def processFile():

    global lineNum
    global assocKey
    global hasFatalError, hasWarningError

    # For each line in the input file

    for line in inputFile.readlines():

        lineNum = lineNum + 1

        # Split the line into tokens
        tokens = line[:-1].split('\t')

        try:
            jnumid = tokens[0]
            mgiid = tokens[1]
            createdBy = tokens[2]
        except:
            errorFile.write('Invalid Line (row %d): %s\n' % (lineNum, line))
            hasFatalError += 1
            continue

        refKey = loadlib.verifyReference(jnumid, lineNum, errorFile)
        createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)
        objectKey, mgiTypeKey, assocTypeKey = verifyObject(mgiid, lineNum)

        assocFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % \
                (assocKey, refKey, objectKey, mgiTypeKey, assocTypeKey, createdByKey, createdByKey, loaddate, loaddate))

        assocKey = assocKey + 1

    #	end of "for line in inputFile.readlines():"

def bcpFiles():
    '''
    # requires:
    #
    # effects:
    #	BCPs the data into the database
    #
    # returns:
    #	nothing
    #
    '''

    # do not process if running sanity check
    if isSanityCheck == 1:
        return

    # do not process if errors are detected
    if hasFatalError > 0:
        errorFile.write("\nCannot process this file.  Sanity check failed\n")
        return

    db.commit()
    assocFile.flush()

    bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh'

    bcp1 = '%s %s %s %s %s %s "|" "\\n" mgd' % \
        (bcpCommand, db.get_sqlServer(), db.get_sqlDatabase(), 'MGI_Reference_Assoc', outputFile, assocFileName)

    diagFile.write('%s\n' % bcp1)

    os.system(bcp1)

    # update mgi_reference_assoc_seq auto-sequence
    db.sql(''' select setval('mgi_reference_assoc_seq', (select max(_Assoc_key) from MGI_Reference_Assoc)) ''', None)
    db.commit()

#
# Main
#

init()
setPrimaryKeys()
processFile()
#bcpFiles()
exit(0)

