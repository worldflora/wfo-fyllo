<?php

// DELETE ME - KEPT FOR REFERENCE TILL FUNCTIONALITY IS PORTED TO AIRFLOW

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

require_once('header.php');

$index = new SolrIndex();

$source_id = @$_GET['source_id'];
$source_id = (int)$source_id;
$offset = @$_GET['offset'];
if(!$offset) $offset = 0;

$previous_indexed_taxa = array(); // keep tabs on ones already in the index


// get the headline details for the facet, facet_value and source
$response = $mysqli->query("SELECT f.`name` as facet_name, f.id as facet_id, fv.`name` as facet_value_name,  fv.id as facet_value_id, s.`name` as source_name, s.id as source_id
FROM facet_value_sources as fvs
JOIN facet_values as fv  on fvs.facet_value_id = fv.id
JOIN facets as f on fv.facet_id = f.id
JOIN sources as s on s.id = fvs.source_id
WHERE fvs.source_id = $source_id;");
$details = $response->fetch_all(MYSQLI_ASSOC)[0];
$response->close();


echo "<h2>Indexing taxa for '{$details['facet_name']}: {$details['facet_value_name']}' data source '{$details['source_name']}' </h2>";

// this is called when we finish paging through those 
// in the database
if(@$_GET['clean_up'] == 'remaining_taxa'){

    // get a list of the remaining ones from the db
    $previous_indexed_taxa = $_SESSION['facet_source_taxa_index'];

    // work through any that were deleted from the source.
    $solr_docs = array();
    foreach($previous_indexed_taxa as $wfo_id){
        $solr_docs[] =  WfoFacets::getTaxonIndexDoc($wfo_id); 
    }
    $index = new SolrIndex();
    $index->saveDocs($solr_docs, true);
    echo "<p>Reindex facet sources.</p>";
    // call again to index the sources
    echo "<script>window.location = \"facet_source_taxa_index.php?source_id={$source_id}&clean_up=index_facet_sources\"</script>";

// this comes at the end of the process if paging etc if ove
}elseif(@$_GET['clean_up'] == 'index_facet_sources'){
    
    // we need to index the source metadata too or it won't be found
    WfoFacets::indexFacetSources();

    // go to the next phase
    echo "<p>Finnished working facet sources metadata.</p>";
    echo "<p>Indexing any new facet scores.</p>";

    // call again to index the scores
    echo "<script>window.location = \"facet_source_taxa_index.php?source_id={$source_id}&clean_up=index_scores\"</script>";

}elseif(@$_GET['clean_up'] == 'index_scores'){
    // also metadata for the individual scores
    // this is data sensitive so will only do the scores that have changed.
    WfoFacets::indexScores();
    echo "<p>Indexing complete. Going back to source page.</p>";

    // finall head back to the source page
    echo "<script>window.location = \"source.php?source_id={$source_id}\"</script>";

}else{

    // we are here so we aren't doing any clean up bit working on actually indexing
    // a page of items from the sql database
    if($offset == 0){

        // we are starting a new run so we get a list of all the
        // things that are already scored to this facet value and datasource in the index.

        // it will have the facet id in the fiel
        $field_name = "wfo-f-{$details['facet_id']}_ss";
        $facet_id = "wfo-fv-{$details['facet_value_id']}";
        $prov_field = "wfo-fv-{$details['facet_value_id']}_provenance_ss";

        // this could be big! fix it if it gets slow. Would be a max of all taxa which is 500,000
        $query = array(
            'query' => "$field_name:$facet_id",
            'filter' => array(
                "classification_id_s:" . WFO_DEFAULT_VERSION
            ),
            "limit" => 1000000, 
            "fields" => array('wfo_id_s', $prov_field)
        );
        $docs = $index->getSolrDocs((object)$query);

        // we only need to save the ones that are from this
        // data source - could do this in SOLR query but not on _ss field type I think
        foreach($docs as $doc){
            foreach($doc->{$prov_field} as $prov){
                if(preg_match("/-{$source_id}-/", $prov)){
                    $previous_indexed_taxa[] = $doc->wfo_id_s;
                    break;
                }
            }
        }

        $_SESSION['facet_source_taxa_index'] = $previous_indexed_taxa;

    }else{
        // on a subsequest page so this is already available
        $previous_indexed_taxa = $_SESSION['facet_source_taxa_index'];
    }

    // get a page full from the db
    $response = $mysqli->query("SELECT distinct(ws.wfo_id) as 'wfo_id' from sources as s
    JOIN wfo_scores as ws on ws.source_id = s.id
    where s.id = $source_id
    order by ws.wfo_id
    LIMIT 100 OFFSET $offset;");

    //echo $response->num_rows;

    if($response->num_rows == 0){

        // declare we are complete
        echo "<p>Finnished working through names.</p>";
        echo "<p>Indexing any previously indexed taxa not in the current data source.</p>";
        // call again to index the remaining taxa.
       
        $url = "facet_source_taxa_index.php?source_id={$source_id}&clean_up=remaining_taxa";
        echo "<script>window.location = \"$url\"</script>";

    }else{

        $solr_docs = array();
        while($row = $response->fetch_assoc()){
            
            $solr_docs[] =  WfoFacets::getTaxonIndexDoc($row['wfo_id']); 

            // remove this wfo_id from the previously indexed
            if (($key = array_search($row['wfo_id'], $previous_indexed_taxa)) !== false) unset($previous_indexed_taxa[$key]);
            
        }

        // save those docs to the index and commit them
        $index = new SolrIndex();
        $index->saveDocs($solr_docs, true);
        
        // put the chopped down list in the session for the next page
        $_SESSION['facet_source_taxa_index'] = $previous_indexed_taxa;

        // tell them what is going on and redirect.
        echo "<p>Indexing in progress. ". number_format($offset, 0)  ." <strong>names</strong> completed.</p>";
        echo "<p>Remaining of previously index <strong>taxa</strong>: ". number_format(count($previous_indexed_taxa), 0)  .".</p>";

        $offset += 100;

        $uri = "facet_source_taxa_index.php?source_id=" . $source_id . "&offset=$offset";
        echo "<script>window.location = \"$uri\"</script>";
        
    }

}// else not doing clean up but indexing pages

require_once('footer.php');
