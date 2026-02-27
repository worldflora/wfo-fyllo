# This takes the latest daily backup  of the db 
# and overwrites the local wfo_facets database with it.
filename=$(ls -tp ../data/db_dumps | grep -v /$ | head -1)
filepath="../data/db_dumps/${filename}"
echo "$filepath"
mysql -e "DROP DATABASE IF EXISTS wfo_facets"
mysql -e "CREATE DATABASE wfo_facets"
start=$(date +"%H:%M:%S")
echo "This may take a while. Starting at $start"
gunzip < $filepath | mysql wfo_facets
end=$(date +"%H:%M:%S")
echo "Finished at $end"
echo "All done!"