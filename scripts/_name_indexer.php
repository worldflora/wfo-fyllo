<?php

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

// the name indexer looks at recently added scores 
// and indexes the associated names ids

$index = new SolrIndex();
$solr_docs = array();

$response = $mysqli->query("SELECT distinct(wfo_id) FROM wfo_facets.wfo_scores where created > CURRENT_DATE() - INTERVAL 1 DAY");

$total_rows = $response->num_rows;
$counted = 0;
while($row = $response->fetch_assoc()){

    $counted++;

    //echo "{$row['wfo_id']}\n";
    $doc = WfoFacets::getTaxonIndexDoc($row['wfo_id']);
    if(!$doc) continue;

    $solr_docs[] = $doc;

    if(count($solr_docs) >= 1000){
        echo "Saving:\t". number_format($counted, 0) . "\t" . number_format($counted/$total_rows * 100 , 0) . "%\n";
        $solr_response = $index->saveDocs($solr_docs, true);
        $solr_docs = array();
    }

}
echo "Saving last one ...\n";
$solr_response = $index->saveDocs($solr_docs, true);
echo "All done!\n";