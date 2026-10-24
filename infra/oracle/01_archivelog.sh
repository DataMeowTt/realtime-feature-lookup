#!/bin/bash
mkdir -p /opt/oracle/oradata/recovery_area

sqlplus -s / as sysdba <<'EOF'
ALTER SYSTEM SET db_recovery_file_dest_size = 10G;
ALTER SYSTEM SET db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' SCOPE=SPFILE;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ARCHIVE LOG LIST
EXIT
EOF
