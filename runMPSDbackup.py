# launch file

import MPSDbackup2ObjectStore.backupMPSD as backupMPSD
import datetime, logging, os, sys
stamp = datetime.datetime.now().strftime("%Y%b%d_%H%M")
logfile = "logfile_" + stamp + ".log"
logging.basicConfig(filename=logfile, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.debug("process start - runMPSDbackup.py")

backupMPSD.BackupMPSD().createBackupFile()