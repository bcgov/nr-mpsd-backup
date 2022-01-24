# launch file
import MPSDbackup2ObjectStore.backupMPSD as backupMPSD
import datetime, logging, os, sys
stamp = datetime.datetime.now().strftime("%Y%b%d_%H%M")
logfile = "logfile_" + stamp + ".log"
logging.basicConfig(filename=logfile, level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)
LOGGER.debug("process start - runMPSDbackup.py")
backup_File = backupMPSD.BackupMPSD().createBackupFile()
LOGGER.debug(f"backup file name created")
LOGGER.debug("Copying Backup File")
backupMPSD.ObjectStore.copyBackupFile(backup_File)
LOGGER.debug(f"backup file copied to S3 bucket")
