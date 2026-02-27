<?php

// DELETE ME

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

require_once('header.php');

$index = new SolrIndex();

$source_id = @$_GET['source_id'];
$source_id = (int)$source_id;
$offset = @$_GET['offset'];
if(!$offset) $offset =0;

$previous_indexed_taxa = array(); // keep tabs on ones already in the index


$response = $mysqli->query("SELECT * FROM sources where id = $source_id");
$details = $response->fetch_all(MYSQLI_ASSOC)[0];
$response->close();

echo "<h2>Indexing taxa from snippet source '{$details['name']}' </h2>";


if(@$_GET['clean_up'] == 'remaining_taxa'){

    // get a list of the remaining ones from the db
    $previous_indexed_taxa = $_SESSION['snippet_source_taxa_index'];

    // work through any that were deleted from the source.
    $solr_docs = array();
    foreach($previous_indexed_taxa as $wfo_id){
        $solr_docs[] =  WfoFacets::getTaxonIndexDoc($wfo_id); 
    }
    $index = new SolrIndex();
    $index->saveDocs($solr_docs, true);

    echo "<p>Reindex snippet sources.</p>";
    // call again to index the sources
    echo "<script>window.location = \"snippet_source_taxa_index.php?source_id={$source_id}&clean_up=index_snippet_sources\"</script>";


// this comes at the end of the process if paging etc if ove
}elseif(@$_GET['clean_up'] == 'index_snippet_sources'){

    WfoFacets::indexSnippetSources();

     // go to the next phase
     echo "<p>Finnished working snippet sources metadata.</p>";
     echo "<p>Indexing any new snippets.</p>";
 
     // call again to index the scores
     echo "<script>window.location = \"snippet_source_taxa_index.php?source_id={$source_id}&clean_up=index_snippets\"</script>";


}elseif(@$_GET['clean_up'] == 'index_snippets'){

    WfoFacets::indexSnippets();

    echo "<p>Indexing complete. Going back to source page.</p>";

    // finall head back to the source page
    echo "<script>window.location = \"snippet_source.php?source_id={$source_id}\"</script>";

}else{

        // we are here so we aren't doing any clean up bit working on actually indexing
        // a page of items from the sql database
        if($offset == 0){

            // this could be big! fix it if it gets slow. Would be a max of all taxa which is 500,000
            $query = array(
                'query' => "source_id_s:$source_id",
                'filter' => array(
                    "classification_id_s:" . WFO_DEFAULT_VERSION,
                    "kind_s:wfo-snippet"
                ),
                "limit" => 1000000, 
                "fields" => array('wfo_id_s')
            );
            $docs = $index->getSolrDocs((object)$query);

            // keep a list of them just as wfo IDs
            foreach($docs as $doc){
                if(isset($doc->wfo_id_s)){
                    $previous_indexed_taxa[] = $doc->wfo_id_s;
                }
            }

            $_SESSION['snippet_source_taxa_index'] = $previous_indexed_taxa;

        }else{
            // on a subsequest page so this is already available
            $previous_indexed_taxa = $_SESSION['snippet_source_taxa_index'];
        }


        // actually page through the snippets to be indexed
        $response = $mysqli->query("SELECT distinct(wfo_id) as 'wfo_id' FROM `snippets` WHERE source_id = $source_id ORDER BY wfo_id LIMIT 10 OFFSET $offset");

        if($response->num_rows == 0){
            echo "<p>Finnished working through names.</p>";
            echo "<p>Indexing any previously indexed taxa not in the current data source.</p>";
            $url = "snippet_source_taxa_index.php?source_id={$source_id}&clean_up=remaining_taxa";
            echo "<script>window.location = \"$url\"</script>";
        }else{

            $solr_docs = array();
            while($row = $response->fetch_assoc()){
            
                $doc =  WfoFacets::getTaxonIndexDoc($row['wfo_id']); 
/*
                if($row['wfo_id'] == 'wfo-0000088131'){
                    echo '<pre>';
                    print_r($doc);
                    echo '</pre>';
                    exit;
                }
*/
               $solr_docs[] = $doc;
               // echo "{$row['wfo_id']}<br/>";
                
                // remove this wfo_id from the previously indexed
                if (($key = array_search($row['wfo_id'], $previous_indexed_taxa)) !== false) unset($previous_indexed_taxa[$key]);
            
            }

            // save those docs to the index and commit them
            $index = new SolrIndex();
            $response = $index->saveDocs($solr_docs, true);
            if(isset($response->error_message)) {
                print_r($response);
                exit;
            }          

            // put the chopped down list in the session for the next page
            $_SESSION['snippet_source_taxa_index'] = $previous_indexed_taxa;
            
            // tell them what is going on and redirect.
            echo "<p>Indexing in progress. ". number_format($offset, 0)  ." completed.</p>";

            $offset += 10;

            $uri = "snippet_source_taxa_index.php?source_id=" . $source_id . "&offset=$offset";
            echo "<script>window.location = \"$uri\"</script>";
            
        }

}

require_once('footer.php');
