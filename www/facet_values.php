<?php
    require_once('header.php');

    $facet_id = (int)$_GET['facet_id'];
    
    $response = $mysqli->query("SELECT * FROM `facets` WHERE id = $facet_id");
    $facet = $response->fetch_assoc();
    $response->close();


        echo '<div style="float: right;">';
        if($user){
            echo '<a class="btn btn-sm btn-outline-secondary" href="facet_edit.php?facet_id='. $facet_id .'" role="button">Edit facet</a>';
            echo '&nbsp;<a class="btn btn-sm btn-outline-danger" href="facet_delete.php?facet_id='. $facet_id .'" role="button">Delete facet</a>';
        }
        echo '</div>';


    echo "<p><a href=\"facets.php\">Fyllo</a> 
        → <a href=\"facets.php\">Facets</a> 
        → {$facet['name']}</p>";
        
    echo "<h1>{$facet['name']}</h1>";
    echo "<p class=\"lead\">{$facet['description']}";

    if($facet['heritable']){
        echo "<strong> [Heritable]</strong>";
    }

    echo '</p>';

    echo '<ul class="list-group">';

    $response = $mysqli->query("SELECT * FROM `facet_values` WHERE `facet_id` = {$facet['id']} ORDER BY `name`;");
    $facet_values = $response->fetch_all(MYSQLI_ASSOC);
    $response->close(); 

    foreach($facet_values as $fv){
        
        echo '<li class="list-group-item">';
        echo "<a id=\"facet_value_{$fv['id']}\" ></a>";
        echo '<div class="row">';
        
        echo '<div class="col">';
        echo "<h3>{$fv['name']}</h3>";
        echo "<p><strong>Sources: </strong>";

        $response = $mysqli->query("SELECT * FROM `sources` as s WHERE s.`facet_value_id` = {$fv['id']} ORDER BY s.`name`;");
        $sources = $response->fetch_all(MYSQLI_ASSOC);
        $response->close();

        if($sources){
            $first = true;
            foreach($sources as $s){
                if(!$first) echo "; ";
                echo "<a href=\"source.php?source_id={$s['id']}\">{$s['name']}</a>";
                $first = false;
            }
        }else{
            echo "None";
        }
 
        echo ".</p>"; // edn sources;

        echo "<p>{$fv['description']}</p>";
        echo '</div>'; // end of col

        // if they are god then show the edit buttons
        
        echo '<div class="col" style="text-align: right;">';
        if($user){
            echo '<a class="btn btn-sm btn-outline-secondary" href="facet_value_edit.php?facet_value_id='. $fv['id'] .'" role="button">Edit value</a>';
            echo '&nbsp;<a class="btn btn-sm btn-outline-secondary" href="source.php?facet_value_id='. $fv['id'] .'" role="button">Create source</a>';
            echo '&nbsp;<a class="btn btn-sm btn-outline-danger" href="#" onclick="alert(\'This currently needs to be done at the database level.\')" role="button">Delete</a>';
        }
        echo '</div>'; // end of col
        echo '</div>'; // end of row
        echo '</li>';
    }

    if($user){
        echo '<li class="list-group-item" style="text-align: right;">';
        echo '<a class="btn btn-sm btn-success" href="facet_value_create.php?facet_id='. $facet['id'] .'" role="button">Add value</a>';
        echo '</li>';
    }
?>
</ul>


<?php
    require_once('footer.php');
?>