<?php

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

// This takes in a facet_value_id and
// removes it from the index
// then adds it in again from the facet store.


$page_size = 1000;
$index = new SolrIndex();
$updated_docs = array(); // page full of docs to be posted to solr
$since = time(); // we don't ever index a document twice after we start this 

$facet_value_id = $argv[1];


// get the facet id and facet value id
$response = $mysqli->query("SELECT f.id as facet_id, f.`name` as facet_name, fv.id as facet_value_id, fv.`name` as facet_value_name FROM facet_values as fv JOIN facets as f on fv.facet_id = f.id WHERE fv.id = $facet_value_id");
$rows = $response->fetch_all(MYSQLI_ASSOC);
$response->close();
$facet_value = $rows[0];

echo "\nProcessing {$facet_value['facet_name']} - {$facet_value['facet_value_name']}\n";

// turn them into solr field name and value
$facet_field_name = "wfo-f-{$facet_value['facet_id']}_ss";
$facet_solr_value = "wfo-fv-{$facet_value['facet_value_id']}";

echo "\nIndex all wfo_id in facet service with facet_value_id = {$facet_value['facet_value_id']}.\n";

$sql = "SELECT distinct(wfo_id) FROM wfo_scores WHERE value_id = $facet_value_id;";
$response = $mysqli->query($sql);
echo $mysqli->error;
echo "\t". $response->num_rows . " found.\n";
$updated_docs = array();
while($row = $response->fetch_assoc()){
    echo "{$row['wfo_id']}";
    $new_doc = WfoFacets::getTaxonIndexDoc($row['wfo_id'], $since);
    if($new_doc){

        if(is_object($new_doc)){

            $accepted = $row['wfo_id'] == $new_doc->wfo_id_s ? 'accepted' : 'synonym';
            echo "\t{$new_doc->wfo_id_s}\t{$accepted}\t{$new_doc->full_name_string_plain_s}";

            // stop it all if we don't have it scored
            if(!isset($new_doc->{$facet_field_name}) || !in_array($facet_solr_value, $new_doc->{$facet_field_name}) ){
                echo "\n NO SCORE\n";
                print_r($new_doc);
                exit;
            }
            $updated_docs[] = $new_doc;
        }else{
            echo "\t Already saved";
        }

    }else{
        echo "\t{$row['wfo_id']}\tNo doc - deprecated or unplaced.";
    }
    echo "\n";
    if(count($updated_docs) > 1000){
        echo "Committing 1000\n";
        $index->saveDocs($updated_docs, true);
        $updated_docs = array();
    }
}
$index->saveDocs($updated_docs, true);
echo "\tsaving...\n";

// this should remove any that don't have the facet_value_id anymore
echo "\nRe-indexing all taxa that have {$facet_field_name}:{$facet_solr_value} and indexed before $since\n";

$solr_query = array(
    'query' => '*:*',
    'filter' => array(
        "classification_id_s:" . WFO_DEFAULT_VERSION, 
        $facet_field_name . ":" . $facet_solr_value,
        "role_s:accepted",
        "facets_last_indexed_i:[* TO $since]" // all the ones before we started 
    ),
    'limit' => $page_size,
    'sort' => 'id asc'
); 


while(true){
    
    $page_start = new DateTime(); 

    // get the next page
    $solr_response = $index->getSolrResponse($solr_query);
    echo "Found: {$solr_response->response->numFound}\n";

    echo "Page starts\n";

    $counter = 0;
    foreach ($solr_response->response->docs as $doc) {
        // we update the whole taxon document.
        $counter++;
        echo "\t$counter\t{$doc->wfo_id_s}\t{$doc->full_name_string_plain_s}\t";
        $new_doc = WfoFacets::getTaxonIndexDoc($doc->wfo_id_s, $since);
        if($new_doc && is_object($new_doc)) $updated_docs[] = $new_doc;
        echo "done\n";
    }

    echo "Page ends\n\n";

    // save this page
    $index->saveDocs($updated_docs, true);
    $updated_docs = array();

    // report progress
    $now = new DateTime();
    $page_secs = $now->getTimestamp() - $page_start->getTimestamp();
    
    $page_duration = floor($page_secs);
    echo "Seconds for page: $page_duration\n";

    // now an estimage
    $average_taxon_time = $page_secs/$page_size;
    $taxa_remaining = $solr_response->response->numFound;
    $secs_remaining = floor($taxa_remaining * $average_taxon_time);
    
    $page_start->modify("+{$secs_remaining} seconds");
    echo "Removal ETA:\t\t";
    echo $page_start->format(DATE_ATOM);
    echo "\n\n";

    // we do this until they have all been updated
    if($solr_response->response->numFound == 0) break;

}

$total_duration = (time() - $since) / 1000;
echo "\nSince: $since\n";
echo "Finished all after $total_duration seconds.\n";