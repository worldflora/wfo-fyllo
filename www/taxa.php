<?php
    require_once('header.php');
    require_once('../include/WfoFacets.php');
    require_once('../include/language_codes.php');

    if(!@$_GET['wfo_id']){
?>

<p><a href="index.php">Fyllo</a> → Taxa</p>
<h1>Taxon index values</h1>
<p class="lead">
    This page will show what values should be indexed for any taxon, given the data currently in Fyllo.
</p>

<form method="POST" action="#">
    <div class="mb-3">
        <input type="txt" class="form-control" id="state_search" name="search" value=""
            placeholder="Type the first few letters of the plant name for suggestions" />
    </div>
</form>

<ul class="list-group" id="state_search_results">
</ul>

<script>
// Listen for key up in the text area and do a search
document.getElementById("state_search").onkeyup = function(e) {
    let name_list = document.getElementById("state_search_results");
    nameLookup(e, name_list, null, null);
};
</script>

<?php 
    } else { // end no wfo_id specified

        $wfo = trim($_GET['wfo_id']);

        echo "<p><a href=\"index.php\">Fyllo</a> → <a href=\"taxa.php\">Taxa</a> → $wfo</p>";

        // we've got a wfo id so let's call out to the graph ql service for the hierarchy

        $query = "query{
            taxonNameById(nameId: \"{$wfo}\") {
                id
                fullNameStringHtml
                currentPreferredUsage{
                    classificationId
                    id
                    classification{
                        year
                        monthName
                        month
                    }
                    hasName{
                        id
                        fullNameStringHtml
                    }
                    hasSynonym{
                        id
                        fullNameStringHtml
                    }
                    path{
                        id
                        hasName{
                        id
                        fullNameStringHtml
                        }
                    }
                }
            }
        }";

        $query_object = (object)array("query" => $query);
        $query_json = json_encode($query_object);

        $headers = array();
        $headers[] = 'Content-Type: application/json';
        $headers[] = 'Authorization: Bearer '. $github_access_token;

        $curl = curl_init(PLANT_LIST_GRAPHQL_URI);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS');
        curl_setopt($curl, CURLOPT_POSTFIELDS, $query_json);
        curl_setopt($curl, CURLOPT_POST, 1);
        curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);

        $json = curl_exec($curl);
        $result = json_decode($json);

        echo "<h2>{$result->data->taxonNameById->fullNameStringHtml}</h2>";

        // check we have an accepted name
        if(!isset($result->data->taxonNameById->currentPreferredUsage) || $result->data->taxonNameById->currentPreferredUsage == null){
            // this is an unplaced name
            echo '<div class="alert alert-warning" role="alert">';
            echo $result->data->taxonNameById->fullNameStringHtml . " is an unplaced name and so its facets and snippets will not contribute to the index.";
            echo '</div>';
        }elseif($result->data->taxonNameById->id != $result->data->taxonNameById->currentPreferredUsage->hasName->id){
            // are a synonym
            echo '<div class="alert alert-warning" role="alert">';
            echo $result->data->taxonNameById->fullNameStringHtml 
                . " is an synonym so its facets and snippets will contribute to the indexing of the accepted name "
                . '<a href="taxa.php?wfo_id=' . $result->data->taxonNameById->currentPreferredUsage->hasName->id . '" />'
                . $result->data->taxonNameById->currentPreferredUsage->hasName->fullNameStringHtml
                . '</a>';
            echo '</div>';
        }else{
            echo "<p>These are the data that will be included in the index.</p>";
        }

        if(isset($result->data->taxonNameById->currentPreferredUsage->classification)){
            $classification = $result->data->taxonNameById->currentPreferredUsage->classification;
            echo "<p>Based on classification <strong>{$classification->monthName} {$classification->year}</strong>.</p>";
        }

        // FACET VALUE DATA RENDER
        echo '<hr/>';
        echo '<h3>Facet data</h3>';
        echo '<ul class="list-group list-group-flush">';
        // Work down the path collecting all the facets and data that
        if(isset($result->data->taxonNameById->currentPreferredUsage->path)){

            $path = array_reverse($result->data->taxonNameById->currentPreferredUsage->path);
            foreach ($path as $ancestor) {
                render_name_values($wfo, $ancestor->hasName, true);
            }
        } // end if working through path

        // work through any synonyms
        if(isset($result->data->taxonNameById->currentPreferredUsage->hasSynonym)){
            foreach ($result->data->taxonNameById->currentPreferredUsage->hasSynonym as $synonym) {
                render_name_values($wfo, $synonym, false);
            }
        }
        // close of the last list of facet values
        echo '</ul>';

        // TEXT SNIPPET DATA RENDER
        echo '<hr/>';
        echo '<h3>Snippet data</h3>';
        echo '<ul class="list-group list-group-flush">';

        // this isn't inherited so we just render the name and its synonyms
        render_name_snippets($result->data->taxonNameById->currentPreferredUsage->hasName, false);

        // work through any synonyms
        if(isset($result->data->taxonNameById->currentPreferredUsage->hasSynonym)){
            $has_values = false;
            foreach ($result->data->taxonNameById->currentPreferredUsage->hasSynonym as $synonym) {
                if(render_name_snippets($synonym, true)) $has_values = true;
            }

            if(!$has_values){
                // we never rendered any values to put an explanation in.
                echo "<li class=\"list-group-item\">No linked snippets.</li>";
            }

        }
        // close of the last list of facet values
        echo '</ul>';

        echo '<hr/>';
        echo "<pre>";
      // print_r($result);
        echo "</pre>";
        
    }
?>

</div>
</div>

<?php

    function render_name_snippets($record, $is_synonym){

        global $mysqli;
        global $language_codes_alpha3;

        $has_values = false;

        // fetch all the snippets for this name
        $sql = "SELECT * 
            FROM snippets AS sn
            JOIN sources as so on sn.source_id = so.id
            WHERE sn.wfo_id = '{$record->id}'
            ORDER BY so.snippet_category, so.snippet_language, so.`name`;";

        //echo $sql;

        $response = $mysqli->query($sql);
        $rows = $response->fetch_all(MYSQLI_ASSOC);

        // only render if we have rows
        if($rows){

            $has_values = true;

            if(!$is_synonym) $badge =  '<span class="badge rounded-pill bg-primary">Direct Snippets</span>';
            else $badge = '<span class="badge rounded-pill bg-secondary">Synonym Snippets</span>';

            echo "<li class=\"list-group-item list-group-item-primary\">{$record->fullNameStringHtml} {$badge}</li>";

            $language = null;
            $category = null;
            foreach ($rows as $row) {

                if($language != $row['snippet_language'] || $category != $row['snippet_category'] ){
                    
                    // we've changed so add new title row

                    $language = $row['snippet_language'];
                    $category = $row['snippet_category']; 

                    $category_display = ucfirst($category);
                    $language_display = $language_codes_alpha3[$language]['eng'];
                    
                    
                    echo "<li class=\"list-group-item list-group-item-info\"><strong><a href=\"/snippets.php?category={$category}\">$category_display</a>:</strong> <a href=\"/snippets.php?language={$language}\">$language_display</a></li>";

                }

            }

            // one line for the body
            echo "<li class=\"list-group-item\">{$row['body']}</li>";
            // one line for the source
            echo "<li class=\"list-group-item\"><strong>Source:</strong> <a href=\"source.php?source_id={$row['source_id']}\">{$row['name']}</a></li>";
 
            //print_r($rows);

        }

        return $has_values;
        
    }

    function render_name_values($target_wfo, $record, $require_heritability = true){

        global $mysqli;

        $rec_wfo = $record->id;

        // require heritable if we are not the target taxon set to false when doing synonyms
        $heritable = $require_heritability && $rec_wfo != $target_wfo ?  'AND f.heritable = 1' : '';

        $sql = "SELECT 
            f.id as facet_id,
            f.`name` as facet_name,
            fv.id as facet_value_id, 
            fv.`name` as facet_value_name,
            s.id as source_id,
            s.`name` as source_name
            FROM wfo_scores as ws
            JOIN facet_values AS fv ON ws.value_id = fv.id
            JOIN facets AS f ON fv.facet_id = f.id
            JOIN sources AS s ON ws.source_id = s.id
            WHERE ws.wfo_id = '{$rec_wfo}'
            $heritable
            order by f.`name`, fv.`name`;";

        //echo $sql;

        $response = $mysqli->query($sql);
        $rows = $response->fetch_all(MYSQLI_ASSOC);

        // if we have some values or this is the target taxon we print it out
        if($rows || $rec_wfo == $target_wfo){

            if($rec_wfo == $target_wfo){
                $inherited_badge =  '<span class="badge rounded-pill bg-primary">Direct Values</span>';
            }else{
                if($require_heritability){
                    $inherited_badge =  '<span class="badge rounded-pill bg-secondary">Inherited Values</span>';
                }else{
                    $inherited_badge =  '<span class="badge rounded-pill bg-secondary">Synonym Values</span>';
                }
            }

            echo "<li class=\"list-group-item list-group-item-primary\">{$record->fullNameStringHtml} $inherited_badge</li>";

            // if we don't have any rows (we will be the target taxon) then tell them no values
            if(!$rows) echo "<li class=\"list-group-item\">No directly scored values.</li>";

            $current_f_id = null;
            $current_fv_id = null;
            foreach ($rows as $facet_value) {
                // new facet
                if($current_f_id != $facet_value['facet_id'] || $current_fv_id != $facet_value['facet_value_id']){
                    echo "<li class=\"list-group-item list-group-item-info\"><strong><a href=\"facet_values.php?facet_id={$facet_value['facet_id']}\">{$facet_value['facet_name']}</a>: </strong><a href=\"facet_values.php?facet_id={$facet_value['facet_id']}#fv{$facet_value['facet_value_id']}\">{$facet_value['facet_value_name']}</a></li>";
                    $current_f_id = $facet_value['facet_id'];
                    $current_fv_id = $facet_value['facet_value_id'];
                }
                
                // write the source
                echo "<li class=\"list-group-item\"><strong>Source:</strong> <a href=\"source.php?source_id={$facet_value['source_id']}\">{$facet_value['source_name']}</a></li>";
            }

        }


    }


    require_once('footer.php');
?>