<?php
require_once('../config.php');
require_once('../include/NameCache.php');
require_once('../include/ImporterFacets.php');
require_once('../include/ImporterSnippets.php');


// a chunk of code that shows the progress loading the file uploaded.
$out = array();
$out['timestamp'] = time();

$importer = unserialize(@$_SESSION['importer']);

$page_size = 100;

if($importer){
    $rows_processed = $importer->import($page_size);
    
    // present a nice progress time
    $now = time();
    $elapse =  $now - $importer->created;
    $dt1 = new DateTime("@0");
    $dt2 = new DateTime("@$elapse");
  
    // Calculate the difference between the two timestamps
    $diff = $dt1->diff($dt2);
    
    // Format the difference to display days, hours, minutes, and seconds
    $elapse =  $diff->format('%h hours, %i minutes, %s seconds');
    $progress = number_format($importer->offset, 0);

    if($rows_processed < $page_size){
        $out['message'] = "<strong>Finished: </strong>Rows processed: {$progress}  Elapse time: $elapse.";
        $out['complete'] = true;
        $out['level'] = 'success';
        $out['source_id'] = $importer->sourceId;
        unset($_SESSION['importer']);
    }else{
        $out['message'] = "<strong>Importing: </strong>Rows processed: {$progress}  Elapse time: $elapse.";
        $out['complete'] = false;
        $out['level'] = 'warning';
        $out['source_id'] = $importer->sourceId;
        $_SESSION['importer'] = serialize($importer);
    }
}else{
    $out['message'] = "<strong>Error: </strong>Importer object not available.";
    $out['complete'] = true;
    $out['level'] = 'danger';
    $out['source_id'] = $importer->sourceId;
    unset($_SESSION['importer']);
}

header('Content-Type: application/json');
echo json_encode((object)$out);