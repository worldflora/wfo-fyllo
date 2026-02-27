<?php
    require_once('header.php');

    if(!$user){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }

    $name = trim(@$_REQUEST['name']);
    $description = @$_REQUEST['description'];
    $link_uri = @$_REQUEST['link_uri'];
    $facet_id = (int)@$_REQUEST['facet_id'];

    $response = $mysqli->query("SELECT * FROM `facets` WHERE id = $facet_id");
    $facet = $response->fetch_assoc();
    $response->close();

    if($_POST){

        // check username string OK
        if(strlen($name) > 3){
                

            $name_safe = $mysqli->real_escape_string($name);
            $description_safe = $mysqli->real_escape_string($description);
            $link_uri_safe = $mysqli->real_escape_string($link_uri);

            $mysqli->query("INSERT INTO `facet_values` (`name`, `description`, `link_uri`, `facet_id`) VALUES ('$name_safe', '$description_safe', '$link_uri_safe', $facet_id);");
            if($mysqli->error){
                echo '<div class="alert alert-danger" role="alert">Database insert failed.</div>';
                echo '<div class="alert alert-danger" role="alert">' . $mysqli->error . '</div>';
            }else{
                echo '<div class="alert alert-success" role="alert">Facet "' . $name . '" created.</div>';
                echo "<script>window.location.href = \"facet_values.php?facet_id={$facet['id']}\"</script>";
            }
                
        }else{
            echo '<div class="alert alert-danger" role="alert">The name must be greater than 3 characters long.</div>';
        }
    }

?>

<h1>Create facet value for <?php echo $facet['name']; ?></h1>
<p class="lead">
<form method="POST" action="facet_value_create.php">

    <input type="hidden" name="facet_id" value="<?php echo $facet['id'] ?>" />

    <div class="mb-3">
        <label for="facet_name" class="form-label">Facet value name</label>
        <input type="txt" class="form-control" id="name" name="name" aria-describedby="name_help"
            value="<?php echo $name ?>" />
        <div id="name_help" class="form-text">Keep it short and meaningful.</div>
    </div>

    <div class="mb-3">
        <label for="description" class="form-label">Facet value description</label>
        <textarea class="form-control" id="description" name="description"
            aria-describedby="description_help"><?php echo $description ?></textarea>
        <div id="description_help" class="form-text">A concise description of the facet value.</div>
    </div>

    <div class="mb-3">
        <label for="link_uri" class="form-label">Link URL</label>
        <input type="txt" class="form-control" id="link_uri" name="link_uri" aria-describedby="link_uri_help"
            value="<?php echo $link_uri ?>" />
        <div id="link_uri_help" class="form-text">A link to more information about the facet value.</div>
    </div>

    <button type="submit" class="btn btn-primary">Create</button>
</form>
</p>

<?php
    require_once('footer.php');
?>