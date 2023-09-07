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
    select a._object_key, a._mgitype_key 
    from ACC_Accession  a, MRK_Marker aa
    where a._logicaldb_key = 1
    and a._mgitype_key in (2) 
    and a.preferred = 1 
    and a.accid = \'%s\'
    and a._object_key = aa._Marker_key
    and aa._marker_status_key = 1
    union
    select a._object_key, a._mgitype_key 
    from ACC_Accession a, PRB_Strain aa
    where a._logicaldb_key = 1
    and a._mgitype_key in (10) 
    and a.preferred = 1 
    and a.accid = \'%s\'
    and a._object_key = aa._strain_key
    and aa.private = 0
    union
    select a._object_key, a._mgitype_key 
    from ACC_Accession a, ALL_Allele aa
    where a._logicaldb_key = 1
    and a._mgitype_key in (11) 
    and a.preferred = 1 
    and a.accid = \'%s\'
    and a._object_key = aa._allele_key
    and aa._allele_status_key in (847114, 3983021)
    ''' % (mgiid, mgiid, mgiid), 'auto')

    if len(results) == 0:
            errorFile.write('Invalid Object (row %d): %s\n' % (lineNum, mgiid))
            hasFatalError += 1
    elif len(results) > 1:
            errorFile.write('Object Returns > 1 result (row %d): %s\n' % (lineNum, mgiid))
            hasFatalError += 1
    else:
        r = results[0]
        mgitypeKey = r['_mgitype_key']
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

    errorFile.write('Note:\n')
    errorFile.write('\tif Object = Allele, then must be Approved or Autoload\n')
    errorFile.write('\tif Object = Marker, then must be Official\n')
    errorFile.write('\tif Object = Strain, then must be Public\n')
    errorFile.write('\n')

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

        # if sanity check only, skip/continue
        if isSanityCheck == 1:
                continue

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

