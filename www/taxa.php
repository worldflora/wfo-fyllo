<?php
    require_once('header.php');
    require_once('../include/WfoFacets.php');
    require_once('../include/language_codes.php');

    if(!@$_GET['wfo_id']){
?>

<h1>Index State</h1>
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

        // we've got a wfo id so let's call out to the graph ql service for the hierarchy

        $query = "query{
            taxonNameById(nameId: \"{$wfo}\") {
                id
                fullNameStringHtml
                currentPreferredUsage{
                    classificationId
                    id
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
        $headers[] = 'Authorization: Bearer '.$github_access_token;

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


        // Work down the path collecting all the facets and data that
        if(isset($result->data->taxonNameById->currentPreferredUsage->path)){
            $path = array_reverse($result->data->taxonNameById->currentPreferredUsage->path);
            foreach ($path as $ancestor) {

                $anc_wfo = $ancestor->hasName->id;

                // require heritable if we are not the target taxon
                $heritable = $anc_wfo == $wfo ? '' : 'AND f.heritable = 1';

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
                    WHERE ws.wfo_id = '{$anc_wfo}'
                    $heritable
                    order by f.`name`, fv.`name`;";

                $response = $mysqli->query($sql);
                $rows = $response->fetch_all(MYSQLI_ASSOC);

                // if we have some values or this is the target taxon we print it out
                if($rows || $anc_wfo == $wfo){
                    echo "<p>".  $ancestor->hasName->fullNameStringHtml ."</p>";
                    print_r($rows);
                }

                

            }
        }


        echo '<hr/>';
        echo "<pre>";
        print_r($result);
        echo "</pre>";
        
    }
?>

    </div>
</div>

<?php
    require_once('footer.php');
?>