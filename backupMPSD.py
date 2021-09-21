"""
Backs up the Mines Permitting Spatial Database to object store.

a) Creates a name for the file with a date timestamp in it
b) Backs up the database to name defined in a)
c) Compresses the file if not already compressed
d) Copies the file to the object store
e) Evaluates other files in the object store to identify if any files are older
than a specified retainment window and deletes them if so.

This file contains classes / functions used in process. Called from main script
runMPSDbackup.py

"""
# BIG TO DOs
# LOGGER statements

import datetime, glob, logging, psycopg2, os, sys

# import minio module for writing to S3
import minio
# import scandir if needed, faster for working through directories
import scandir

# import constants file
from . import constants
# not sure why this var name is in UPPERCASE but this is how it's used in the 
# Snowpack scripts
LOGGER = logging.getLogger(__name__)

class BackupMPSD(object):
    """High level functionality for backing up MPSD"""
    def __init__(self):
        LOGGER.debug("init object")
        #TO DO - create dirIterator if necessary, check with Kevin

    def archiveData(self):

    def generateFileName(self):
        # date stamp in MTB GIS format with time. TO DO - doublecheck standards 
        # to ensure consistency with other Jenkins / similar tasks
        time_Stamp = datetime.datetime.now().strftime("%Y%b%d_%H%M")
        # add timestamp to stem name (MPSDBackup) and add .sql to file name
        filename = "MPSDBackup" + time_Stamp + ".sql"
        return filename
       

    def createBackupFile(self):
        # create backup file with timestamp 
        #https://stackoverflow.com/questions/23732900/postgresql-database-backup-using-python
        fileName = self.generateFileName()
        conn = None
        conn = psycopg2.connect(database = contants.POSTGRES_INSTANCE,
                                user = constants.POSTGRES_USER,
                                password = constants.POSTGRES_USER)
        cursor = conn.cursor()
        tables = []
        # the where clause below might not be necessary
        cursor.execute("""SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public'""")
        #https://stackoverflow.com/questions/10598002/how-do-i-get-tables-in-postgres-using-psycopg2
        # Not sure if this will append table as desired string (e.g. is there punctuation?)
        for table in cursor.fetchall():
            tables.append(table)
        # TO DO - get list of tables
        # get each row from each table and write it to an .sql file
        with open(fileName, 'a"') as f:
            for t in tables:
            # string formatting is a bit tricky 
            # https://www.psycopg.org/docs/usage.html#query-parameters
                cursor.execute("""
                    SELECT * FROM %s;", (t,)
                    """)
                # this is another option for the cursor but I'm not sure it'll work
                #SQL = "SELECT * FROM %s"
                #cursor.execute(SQL, t)
                for row in cursor:
                    f.write("INSERT INTO " + t + " values (" + str(row) + ");")
            # this seems redundant now
        backupFile = fileName
        return backupFile


    # not including deleteDir, omitDirs functions from snowpack yet, not sure if 
    # needed

    # isReadyForArchive is likely not needed, that's a time check for >20 days

    # getDirectoryDate may or may not be needed - ask Kevin, think it is to do
    # with directory naming conventions in Snowpack stuff


class DirectoryList(object):
    """Iterator class for directories.

    Provides an iterable, with each iteration gets the path to a new directory
    that needs to be backed up
    """

    # not sure if this is essential to our script since I think we should be 
    # backing everything up anyway
    # Ask Kevin - big picture, should this script just back up everything?

class ObjectStore(object):
    r'''
    Methods to support easy copying of data from filesystem to object store.

    Gets the object store connection information from environment variables:
        * OBJ_STORE_HOST
        * OBJ_STORE_USER
        * OBJ_STORE_SECRET

    Gets the destination directory to copy to from:
        OBJ_STORE_ROOT_DIR



    Gets the source dir that needs to be copied from:
        SRC_ROOT_DIR

    So given the following hypothetical values:
       SRC_ROOT_DIR = C:\nheidecke\WinningNumbers
       OBJ_STORE_ROOT_DIR = nheidecksData\Winnings

    the script will copy the contents of C:\nheideck\WinningNumbers into the
    directory nheidecksData\Winnings in the object store
    '''

    def __init__(self):
        """contructor, sets up object store client, and inits obj store obj.
        """
        self.minIoClient = minio.Minio(os.environ['OBJ_STORE_HOST'],
                                       os.environ['OBJ_STORE_USER'],
                                       os.environ['OBJ_STORE_SECRET'])

        self.objIndex = {}
        self.curDir = None
        self.copyDateDirCnt = 0

    def copyBackupFile(self):
        # my version of copyDirectoryRecursive because I don't think we need to copy
        # recursive directories

        # call create backup file function - TO DO - double check syntax
        backupFile = BackupMPSD.createBackupFile()
        # does this actually execute the backup as intended?
        # TO DO - define object store path
        copyObj = self.minIoClient.fput_object(
            constants.OBJ_STORE_BUCKET,
            objStorePath,
            backupFile,
            part_size=part_size)
        LOGGER.debug(f'copyObj: {copyObj}')

    # def getObjStorePath
    # TO DO - needed - see copyDirectoryRecursive

    # def copy

    # def removeSrcRootDir

    def copyDirectoryRecurive(self, srcDir):
         # Function from Snowpack:
         # I can edit this to not have to worry about directories, 
         # just copy the backup file
        """Recursive copy of directory contents to object store.

        Iterates over all the files and directoris in the 'srcDir' parameter,
        when the iteration finds a directory it calls itself with that
        directory

        If the iteration finds a file, it copies the file to object store then
        compares the object stores md5 with the md5 of the version on file, if
        they match then the src file is deleted.  If a mismatch is found then
        the ValueError is raised.

        :param srcDir: input directory that is to be copied
        :type srcDir: str
        :raises ValueError: if the file has been copied by the md5's do not
            align between source and destination this error will be raised.
        """
        part_size = 15728640
        for local_file in glob.glob(srcDir + '/**'):
            objStorePath = self.getObjStorePath(local_file,
                                                prependBucket=False)
            LOGGER.debug(f"objStorePath: {objStorePath}")
            if not os.path.isfile(local_file):
                self.copyDirectoryRecurive(local_file, objStorePath)
            else:
                if not self.objExists(objStorePath):
                    LOGGER.debug(f"uploading: {local_file} to {objStorePath}")
                    # this is the line that oes the actual copy
                    # syntax: (bucket, destination, source file)
                    copyObj = self.minIoClient.fput_object(
                        constants.OBJ_STORE_BUCKET,
                        objStorePath,
                        local_file,
                        part_size=part_size)
                    LOGGER.debug(f'copyObj: {copyObj}')
                    # might not need the rest of the function if not deleting source
                    etagDest = copyObj[0]
                else:
                    etagDest = self.objIndex[objStorePath]
                md5Src = \
                    hashlib.md5(open(local_file, 'rb').read()).hexdigest()
                LOGGER.debug(f"etagDest: {etagDest}")
    # TO DO - not delete source? Remove?
                if etagDest == md5Src:
                    # delete the source
                    LOGGER.info("md5's of source / dest match deleting the " +
                                f"src: {srcDir}")
                    os.remove(local_file)
                elif (len(etagDest.split('-')) == 2) and \
                        self.checkMultipartEtag(local_file, etagDest):
                    # etag format suggests the file was uploaded as a multipart
                    # which impacts how the etags are calculated
                    LOGGER.info("md5's of source / dest match as multipart " +
                                f"deleting the src: {srcDir}")
                    os.remove(local_file)
                else:
                    # if the md5 doesn't match either file isn't valid on the
                    # s3 side in which case we won't delete the source and we
                    # will throw and error in the log.
                    #    OR
                    # the etag doesn't match because the file was uploaded as a
                    # multipart object

                    msg = f'source: {local_file} and {objStorePath} both ' + \
                          'exists, but md5s don\'t align'
                    LOGGER.error(msg)

    # def checkMultipartEtag

    # def objExists(self, inFile):

      def refreshObjList(self, inFile):
          #need this for the self.minIoClinet.list_objects line