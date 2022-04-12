"""
Backs up the Mines Permitting Spatial Database to object store.

a) Creates a name for the backup file with a date timestamp in it
b) Backs up the database to file defined in a)
c) Compresses the file 
d) Copies the file to the object store

This file contains classes / functions used in process. Called from main script
runMPSDbackup.py

"""

import datetime, gzip, logging, os
from email.mime import base
# import minio module for writing to S3
import minio
from . import constants
# modules for pgdump
from subprocess import PIPE,Popen

LOGGER = logging.getLogger(__name__)

class BackupMPSD(object):
    """High level functionality for backing up MPSD"""
    def __init__(self):
        LOGGER.debug("init object BackupMPSD")

    def generateFileName(self):
        # date stamp in MTB GIS format with time. TO DO - doublecheck 
        # standards to ensure consistency with other Jenkins / similar tasks
        time_Stamp = datetime.datetime.now().strftime("%Y%b%d-%H%M")
        # add timestamp to stem name (MPSDBackup) and add .dmp to file name
        file_Path = os.path.dirname(__file__)
        LOGGER.debug(f"Working folder location: {file_Path}")
        file_Name = f"mpsdbackup_{time_Stamp}.dmp".lower()
        file = os.path.join(file_Path, file_Name)
        return file

    def createBackupFile(self):
        # call abovegenerateFileName function to get name for backup file
        backup_File = self.generateFileName()
        LOGGER.debug(f"Creating backup file: {backup_File}")
        # command to send to subprocess to run pg_Dump with required parameters
        dump_Command = ("pg_dump -Fc" # "custom" format dump
                        f" -h {constants.POSTGRES_HOST}" 
                        f" -d {constants.POSTGRES_DB}"
                        f" -U {constants.POSTGRES_ID}"
                        f" -p {constants.POSTGRES_PORT}"
                        f" -n app_mpsd" # only dump app_mpsd schema
                        f''' --file="{backup_File}"'''
        )
        LOGGER.debug(f"pg_dump command: {dump_Command}")
        # open a subprocess and allow text to be passed into it
        p = Popen(dump_Command, shell=True, stdin=PIPE, stdout=PIPE, 
                stderr=PIPE, text=True)
        # communicate the secret, carriage return is important

        output, errors = p.communicate(f"{constants.POSTGRES_SECRET}\r\n")
        LOGGER.debug(f"Communicate Statement Output: {output}")
        LOGGER.debug(f"Communicate Statement Errors: {errors}")
        compressed_Name = f"{backup_File}.gz"
        with (open(backup_File, "rb") as file_In, 
             gzip.open(compressed_Name, "wb") as file_Out):
            file_Out.writelines(file_In)
        return compressed_Name

class ObjectStore(object):

    r"""
    Methods to support easy copying of data from filesystem to object store.

    Gets the object store connection information from environment variables:
        * OBJ_STORE_HOST
        * OBJ_STORE_USER
        * OBJ_STORE_SECRET
        * OBJ_STORE_BUCKET

    Creates a minIO client and copies compressed file output of createBackupFile
    to object store destination
    """

    def __init__(self):
        LOGGER.debug("init object ObjectStore")

    def copyBackupFile(backup):
        # create client
        minIoClient = minio.Minio(os.environ['OBJ_STORE_HOST'],
                                  os.environ['OBJ_STORE_USER'],
                                  os.environ['OBJ_STORE_SECRET'])
        LOGGER.debug(f"minIo client created")
        LOGGER.debug(f"full backup file path: {backup}")
        # split the file name off the backup file path, need this to name the
        # object in the S3 bucket
        copy_Name = os.path.split(backup)[1]
        LOGGER.debug(f"copy_Name: {copy_Name}")
        # create copy object in bucket using file name 
        copyObj = minIoClient.fput_object(
            constants.OBJ_STORE_BUCKET,
            copy_Name,
            backup)
        LOGGER.debug(f"copyObj: {copyObj}")
        print(f"copyObj: {copyObj}")