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
        echo "<a href=\"facet_values.php?facet_id={$f['id']}\"><h3>{$f['name']}</h3></a>";
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