# will backup the live database as specified in the config file.
# suitable for being run by cron
today=$(date +"%Y-%m-%d-%H-%M-%S")
filename="../data/db_dumps/fyllo_${today}.sql"
mysqldump fyllo > $filename
echo $filename
gzip $filename

# prevent backups filling disk
find ../data/db_dumps -type f -mtime +10 -delete


