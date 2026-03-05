<?php
    require_once('header.php');
    require_once('../include/WfoFacets.php');
    require_once('../include/language_codes.php');

    if(!@$_GET['wfo_id']){
?>

<h1>Index State</h1>
<p class="lead">
    Here you can see the relationship between facets scored to names and what this will translate to in and index. Search for a name and click on 'inspect'.
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

    // load the complete record from solr
   
    
    
    $tree = WFOFacets::getTaxonTree($_GET['wfo_id']);
    
//    echo '<pre>';
  //  print_r($tree);
    //echo '</pre>';

    echo "<p style=\"text-align: right;\"><a href=\"https://list.worldfloraonline.org/{$index_name->wfo_id_s}\" target=\"wfo\">{$index_name->wfo_id_s} ↗</a></p>";
    echo "<h1 style=\"border-bottom: solid 1px black;\">{$index_name->full_name_string_html_s}</h1>";
    
    // the facet services stuff comes on the left
    echo '<div class="row align-items-start">';
    echo '<div class="col">';
    echo '<h2  style="border-bottom: solid 1px black;">Values in Fyllo</h2>';
    echo '<p>These are the values for <strong>just this name</strong> here in the facet service.</p>';

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
        WHERE ws.wfo_id = '{$index_name->wfo_id_s}'
        order by f.`name`, fv.`name`;";

    $response = $mysqli->query($sql);
    $rows = $response->fetch_all(MYSQLI_ASSOC);
    $current_facet_id = null;
    foreach($rows as $row){
        
        // change face
        if($current_facet_id != $row['facet_id']){
            if($current_facet_id != null) echo '</ul>';
            echo "<h3>{$row['facet_name']}</h3>";
            echo '<ul>';
            $current_facet_id = $row['facet_id'];
        }

        echo "<li>";
        echo "<a href=\"facet_values.php?facet_id={$row['facet_id']}#fv{$row['facet_value_id']}\">{$row['facet_value_name']}</a>";
        echo "<strong> from </strong>";
        echo "<a href=\"source.php?source_id={$row['source_id']}\">{$row['source_name']}</a>";
        echo "</li>";

    }
    echo "</ul>"; // end last facet value list
    $response->close();


    // now the text snippets
    $sql = "SELECT 
	s.id, s.body, so.`snippet_category`, so.`snippet_language`, so.`name` as source_name, so.id as source_id 
FROM wfo_facets.snippets as s 
JOIN sources as so on s.source_id = so.id
WHERE s.wfo_id = '{$index_name->wfo_id_s}'
order by so.snippet_category, so.snippet_language;";

$response = $mysqli->query($sql);

$lang = null;
$cat = null;
while($row = $response->fetch_assoc()){

    if($cat != $row['category'] || $lang != $row['language']){
        $cat = $row['category'];
        $lang = $row['language'];
        echo "<h3>" . ucfirst($cat) . " - ". $language_codes_alpha3[$lang]['eng'] ."</h3>";
    }

    echo "<p><a href=\"snippet_source.php?source_id={$row['source_id']}\">{$row['source_name']}</a></p>";
    echo "<p>{$row['body']}</p>";

}


    echo '</div>'; // end left column
?>


<div class="col" style="border-left: solid 1px black; width: 50%;">
    <h2 style="border-bottom: solid 1px black;">Associated Index</h2>
    <p>
        These are the <strong>calculated values</strong> for this <strong>taxon</strong> in the index.
    <?php 

        if($index_name->role_s != 'accepted'){
            echo " This name is a '{$index_name->role_s}' name so doesn't have facets in the index.";
        }
         
        if(isset($index_name->facets_last_indexed_i)){
            $date = date(DATE_ATOM, $index_name->facets_last_indexed_i);
            echo "<br/>Last indexed on: {$date}</p>";
        }else{
            echo '<br/>Never indexed. </p>';
        }

        $facets = WfoFacets::getFacetsFromDoc($index_name);

        foreach($facets as $facet_id => $facet){
            echo "<h4>{$facet['meta']['name']}</h4>";
            echo '<ul>';
            foreach ($facet['facet_values'] as $facet_value_id => $facet_value) {
                echo '<li><strong>';
                echo $facet_value['meta']->name;
                echo '</strong><br/>based on:';
                echo '<ul>';
                foreach($facet_value['provenance'] as $prov){
                    echo '<li>';
                    echo "<strong>{$prov['kind']}&nbsp;</strong>";
                    echo $prov['full_name_html'];
                    echo "<strong> by </strong>{$prov['source_name']}";
                    echo '</li>';  
                }
                echo '</ul>';
                echo '</li>';
            }
            echo '</ul>';
            
        } // each facet


        if(isset($index_name->snippet_text_bodies_txt)){
            // now the text snippets
            for ($i=0; $i < count($index_name->snippet_text_bodies_txt); $i++) { 
                    
                echo "<h3>" . ucfirst($index_name->snippet_text_categories_ss[$i]) . " - ". $language_codes_alpha3[$index_name->snippet_text_languages_ss[$i]]['eng'] ."</h3>";
                echo "<p>{$index_name->snippet_text_name_ids_ss[$i]}</p>";
                echo "<p>{$index_name->snippet_text_ids_ss[$i]}</p>";
                echo "<p>{$index_name->snippet_text_bodies_txt[$i]}</p>";
                
            }
        }
    
    ?>
    </pre>
</div>
</div>

<?php


    } // end wfo_id specified

    require_once('footer.php');
?>