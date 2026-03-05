<?php

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

// the taxon indexer works through all the accepted taxa 
// in the index and indexes them.
// used after major changes or when a new classification
// has been deployed.

$page_size = 1000;
$offset = 0;
$index = new SolrIndex();
$updated_docs = array();
$run_start = new DateTime(); 

if(@$argv[1]) $offset = $argv[1];

$solr_query = array(
    'query' => '*:*',
    'filter' => array("classification_id_s:" . WFO_DEFAULT_VERSION, "role_s:accepted"),
    'fields' => array("wfo_id_s", "full_name_string_plain_s"),
    'limit' => $page_size,
    'offset' => $offset,
    'sort' => 'id asc'
); 

while(true){
    
    $page_start = new DateTime(); 

    echo "Page starts: \t" . number_format($offset, 0) . "\n";

    // get the next page
    $solr_query['offset'] = $offset;
    $solr_response = $index->getSolrResponse($solr_query);


    $counter = 0;
    foreach ($solr_response->response->docs as $doc) {
        $counter++;
        $call_timer = microtime(true);
        echo "\t$counter\t{$doc->wfo_id_s}\t{$doc->full_name_string_plain_s}\t";
        $new_doc = WfoFacets::getTaxonIndexDoc($doc->wfo_id_s);        
        if($new_doc) $updated_docs[] = $new_doc;
        echo number_format(microtime(true) - $call_timer, 3) . " msecs\t";
        echo number_format(count(WfoFacets::$facetsCache)) . " cached\tdone\n";
        
    }

    echo "Page ends\n";

    // save this page
    $index->saveDocs($updated_docs, true);
    $updated_docs = array();
    $offset += $page_size;

    // report progress
    $now = new DateTime();
    $page_secs = $now->getTimestamp() - $page_start->getTimestamp();
    
    $page_duration = floor($page_secs);
    echo "Seconds for page: $page_duration\n";

    // now an estimage
    $average_taxon_time = $page_secs/$page_size;
    $taxa_remaining = $solr_response->response->numFound - $offset;
    $secs_remaining = floor($taxa_remaining * $average_taxon_time);
    
    $page_start->modify("+{$secs_remaining} seconds");
    echo "ETA:\t\t";
    echo $page_start->format(DATE_ATOM);
    echo "\n\n";

    // do the next page or stop
    if($offset > $solr_response->response->numFound) break;

}

$now = new DateTime();
$total_duration = $now->getTimestamp() - $run_start->getTimestamp();

$mysqli->query("INSERT INTO indexing_log (`kind`, `count`, `duration`) VALUES ('taxon',{$solr_response->response->numFound}, $total_duration);");
echo $mysqli->error;

echo "Finished all\n";