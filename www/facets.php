<?php
    require_once('header.php');
?>
<p><a href="index.php">Fyllo</a> → Facets</p>
<h1>Facets</h1>
<p class="lead">
    These are the facets in the system.
</p>
<ul class="list-group">

    <?php
    $response = $mysqli->query("SELECT * FROM `facets` ORDER BY `name`;");
    $facets = $response->fetch_all(MYSQLI_ASSOC);
    $response->close();
    foreach($facets as $f){

        $response = $mysqli->query("SELECT count(*) as n FROM `facet_values` WHERE `facet_id` = {$f['id']};");
        $row = $response->fetch_assoc();
        $count = $row['n'];
        $response->close();

        echo '<li class="list-group-item">';
        echo "<h3><a href=\"facet_values.php?facet_id={$f['id']}\">{$f['name']}</a>";
        if($f['heritable']){
                echo '&nbsp;<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-sort-down-alt" viewBox="0 0 16 16">
            <path d="M3.5 3.5a.5.5 0 0 0-1 0v8.793l-1.146-1.147a.5.5 0 0 0-.708.708l2 1.999.007.007a.497.497 0 0 0 .7-.006l2-2a.5.5 0 0 0-.707-.708L3.5 12.293zm4 .5a.5.5 0 0 1 0-1h1a.5.5 0 0 1 0 1zm0 3a.5.5 0 0 1 0-1h3a.5.5 0 0 1 0 1zm0 3a.5.5 0 0 1 0-1h5a.5.5 0 0 1 0 1zM7 12.5a.5.5 0 0 0 .5.5h7a.5.5 0 0 0 0-1h-7a.5.5 0 0 0-.5.5"/>
            </svg>';
            echo '<sup><span style="font-size: 50%;">Heritable</span></sup>';      
        }
        echo "</h3>";
        echo "<p><strong>Number of values: </strong> $count</p>";
        echo "<p>{$f['description']}</p>";
        echo '</li>';
    }

    // if god the can create new facets
    if($user){
        echo '<li class="list-group-item" style="text-align: right;">';
        echo '<a class="btn btn-sm btn-success" href="facet_create.php" role="button">Add facet</a>';
        echo '</li>';
    } // is god
?>
</ul>


<?php
    require_once('footer.php');
?>