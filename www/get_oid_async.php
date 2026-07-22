<?php
require_once('../config.php');
require_once('../include/FileStore.php');

$store = new FileStore($_GET['file_path']);

if($store->file){
    echo $store->file->oid;
}else{
    echo "Can't find the file in GitHub!";
}


