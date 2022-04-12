# launch file
import MPSDbackup2ObjectStore.backupMPSD as backupMPSD
import logging, os

# configure the logger
# message level, message string output format, handler for console
str_Fmt = ("%(asctime)s - %(name)s - %(levelname)s -" 
            "line %(lineno)d\nmsg: %(message)s\n")
logging.basicConfig(
    level=logging.DEBUG,
    format=str_Fmt,
    handlers=[logging.StreamHandler()]
)
# instantiate logger
LOGGER = logging.getLogger(__name__)

LOGGER.debug("process start - runMPSDbackup.py")
backup_File = backupMPSD.BackupMPSD().createBackupFile()
if os.path.exists(backup_File):
    LOGGER.debug(f"backup file created")
else:
    LOGGER.error(f"BACKUP FILE NOT CREATED")

LOGGER.debug("Copying Backup File to S3 Bucket")
try:
    backupMPSD.ObjectStore.copyBackupFile(backup_File)
    LOGGER.debug(f"backup file copied to S3 bucket")
except:
    LOGGER.error(f"BACKUP FILE NOT COPIED TO S3 BUCKET")
