import subprocess
import shlex
import csv
import sys
import os

dump_file = open('tg_messages_v2.sql', 'w')
log = open('log.txt', 'w')

# Executing pg_dump command
dump_cmd = shlex.split('pg_dump -U tg_user -h tg-t-a.is-systems.org tg_messages_v2')
process_map = subprocess.Popen(dump_cmd, stdout=dump_file, stderr=log)
process_map.communicate()
log.flush()

# Get list of active sessions and write output to a csv file
session_cmd = shlex.split('psql -Umyuser -dpostgres -c "\copy (SELECT * FROM pg_stat_activity '
                          'WHERE datname = \'tg-messages-v3\') to \'sessions.csv\' with csv"')
process_map = subprocess.Popen(session_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()

# Read csv file and turn PIDs into list
pid = []
with open('sessions.csv', 'r') as file:
    reader = csv.reader(file)
    for each_row in reader:
        pid.append(each_row[2])
        print(pid)

# Check if file with sessions is not empty
if os.stat('sessions.csv').st_size > 0:
    # For each PID execute command to terminate session
    for each in pid:
        killsession_cmd = shlex.split('psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
                                      'WHERE pid = ' + each + ';"')
        process_map = subprocess.Popen(killsession_cmd, stdout=log, stderr=log)
        process_map.communicate()
        log.flush()
else:
    with open('log.txt', 'a') as sys.stdout:
        print('There is no active sessions')

# Delete test database
drop_cmd = shlex.split('psql -Umyuser -dpostgres -c \'DROP DATABASE "tg-messages-v3";\'')
process_map = subprocess.Popen(drop_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()

# Create new empty database
createdb_cmd = shlex.split('psql -Umyuser -dpostgres -c "CREATE DATABASE tg_messages_v2 WITH OWNER = myuser;"')
process_map = subprocess.Popen(createdb_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()

# Executing dump restore
restore_cmd = shlex.split('psql -Umyuser -dtg_messages_v2 -f tg_messages_v2.sql')
process_map = subprocess.Popen(restore_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()

# Rename test database
restore_cmd = shlex.split('psql -Umyuser -dpostgres -c \'alter database tg_messages_v2 rename to "tg-messages-v3";\'')
process_map = subprocess.Popen(restore_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()

# Create indexes messages_attach and messages_clean with link to elasticsearch
messclean_cmd = shlex.split('psql -Umyuser -dtg-messages-v3 -c "CREATE INDEX idxmessages_clean ON public.messages_clean '
                            'USING zombodb ((messages_clean.*)) WITH (url=\'http://172.28.0.3:9200/\');"')
process_map = subprocess.Popen(messclean_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()

messattach_cmd = shlex.split('psql -Umyuser -dtg-messages-v3 -c "CREATE INDEX idxmessage_attach ON public.messages_attach '
                             'USING zombodb ((messages_attach.*)) WITH (url=\'http://172.28.0.3:9200/\');"')
process_map = subprocess.Popen(messattach_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()

# Clean directory
rm_cmd = shlex.split('rm log.txt sessions.csv tg_messages_v2.sql')
process_map = subprocess.Popen(rm_cmd, stdout=log, stderr=log)
process_map.communicate()
log.flush()
