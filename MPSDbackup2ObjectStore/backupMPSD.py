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

import datetime, glob, gzip, logging, psycopg2, os, sys, time
# import minio module for writing to S3
import minio
# import scandir if needed, faster for working through directories
import scandir
from . import constants
# modules for pgdump
from subprocess import PIPE,Popen

LOGGER = logging.getLogger(__name__)

class BackupMPSD(object):
    """High level functionality for backing up MPSD"""
    def __init__(self):
        LOGGER.debug("init object")

    def generateFileName(self):
        # date stamp in MTB GIS format with time. TO DO - doublecheck 
        # standards to ensure consistency with other Jenkins / similar tasks
        time_Stamp = datetime.datetime.now().strftime("%Y%b%d_%H%M")
        # add timestamp to stem name (MPSDBackup) and add .sql to file name
        file_Path = constants.WORKING_FOLDER
        LOGGER.debug(f"Working folder location: {file_Path}")
        file_Name = "MPSDBackup" + time_Stamp  + ".dmp"
        file = os.path.join(file_Path, file_Name)
        return file

    def createBackupFile(self):
        backup_File = self.generateFileName()
        LOGGER.debug(f"Creating backup file: {backup_File}")
        dump_Command = (" pg_dump -Fc" # "custom" format dump
                        f" -h {constants.POSTGRES_HOST}" 
                        f" -d {constants.POSTGRES_DB}"
                        f" -U {constants.POSTGRES_ID}"
                        f" -p {constants.POSTGRES_PORT}"
                        f" -n app_mpsd" # only dump app_mpsd schema
                        f" --file={backup_File}" # line for everything else
        )
        LOGGER.debug(f"pg_dump command: {dump_Command}")
        # open a subprocess and allow text to be passed into it
        p = Popen(dump_Command, shell=True, stdin=PIPE, stdout=PIPE, 
                stderr=PIPE, text=True)
        # communicate the secret, carriage return is important
        output, errors = p.communicate(f"{constants.POSTGRES_SECRET}\r\n")
        LOGGER.debug(f"Communicate Statement Output: {output}")
        LOGGER.debug(f"Communicate Statement Errors: {errors}")
        compressed_Name = f"{backup_File}" + ".gz"
        with (open(backup_File, "rb") as file_In, 
             gzip.open(compressed_Name, "wb") as file_Out):
            file_Out.writelines(file_In)
        return compressed_Name

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
        LOGGER.debug("init object")

    def copyBackupFile(backup):
        minIoClient = minio.Minio(os.environ['OBJ_STORE_HOST'],
                                  os.environ['OBJ_STORE_USER'],
                                  os.environ['OBJ_STORE_SECRET'])
        LOGGER.debug(f"minIo client created")
        LOGGER.debug(f"full backup file path: {backup}")
        copy_Name = os.path.split(backup)[1]
        LOGGER.debug(f"copy_Name: {copy_Name}")

        copyObj = minIoClient.fput_object(
            constants.OBJ_STORE_BUCKET,
            copy_Name,
            backup)
        LOGGER.debug(f"copyObj: {copyObj}")
        print(f"copyObj: {copyObj}")