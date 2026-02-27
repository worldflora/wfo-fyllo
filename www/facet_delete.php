<?php
    require_once('header.php');

    if(!$user){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }

    $facet_id = (int)@$_REQUEST['facet_id'];
    
    // do the deletion if we are posted to
    if($_POST){
        $mysqli->query("DELETE FROM `facets` WHERE id = $facet_id;");

        if($mysqli->error){
            echo '<div class="alert alert-danger" role="alert">Database deletion failed.</div>';
            echo '<div class="alert alert-danger" role="alert">' . $mysqli->error . '</div>';
        }else{
            echo '<div class="alert alert-success" role="alert">Facet deleted.</div>';
            echo "<script>window.location.href = \"facets.php\"</script>";
        }
    }

    // we are GET and so loading
    $response = $mysqli->query("SELECT * FROM `facets` WHERE id = $facet_id");
    $facet = $response->fetch_assoc();
    $response->close();

    $name = $facet['name'];
    $description = $facet['description'];
    $link_uri = $facet['link_uri'];

    $response = $mysqli->query("SELECT count(*) as n FROM `facet_values` WHERE facet_id = $facet_id");
    $row = $response->fetch_assoc();
    $value_count = $row['n'];
    $response->close();

    // FIXME: Add in the number of scores and sources that will be deleted.

?>
<div class="alert alert-danger" role="alert">Are you about to do something mad, bad and dangerous?</div>

<h1>Delete Facet: <?php echo $name ?></h1>
<p class="lead">
    You are about to delete a facet. This will cause a cascade delete. It means all the attached facet values
    (<?php echo $value_count ?>) and all
    the sources and their associated
    data linked to the facet values will also be deleted!
</p>
<form method="POST" action="facet_delete.php">
    <h2>Are you sure?

        <input type="hidden" name="facet_id" value="<?php echo $facet_id ?>" />
        <button type="submit" class="btn btn-primary">Yes delete it</button>

    </h2>
</form>
<?php
    require_once('footer.php');
?>