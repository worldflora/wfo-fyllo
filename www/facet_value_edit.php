<?php
    require_once('header.php');

    if(!$user){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }

    $facet_value_id = (int)@$_REQUEST['facet_value_id'];

    $response = $mysqli->query("SELECT * FROM `facet_values` WHERE id = $facet_value_id");
    $facet_value = $response->fetch_assoc();
    $response->close();

    $response = $mysqli->query("SELECT * FROM `facets` WHERE id = {$facet_value['facet_id']}");
    $facet = $response->fetch_assoc();
    $response->close();

    if($_POST){

        // check username string OK
      
        $name_safe = $mysqli->real_escape_string($_POST['name']);
        $description_safe = $mysqli->real_escape_string($_POST['description']);
        $link_uri_safe = $mysqli->real_escape_string($_POST['link_uri']);
        $code_safe = $mysqli->real_escape_string($_POST['code']);

        $mysqli->query("UPDATE `facet_values` SET `name` = '$name_safe', `description` = '$description_safe', `link_uri` = '$link_uri_safe', `code` = '$code_safe' WHERE id = {$facet_value['id']};");
        if($mysqli->error){
            echo '<div class="alert alert-danger" role="alert">Database update failed.</div>';
            echo '<div class="alert alert-danger" role="alert">' . $mysqli->error . '</div>';
        }else{
            echo '<div class="alert alert-success" role="alert">Update successful.</div>';
            echo "<script>window.location.href = \"facet_values.php?facet_id={$facet['id']}#facet_value_{$facet_value['id']}\"</script>";
        }

    }


?>

<h1><?php echo $facet['name'] ?>: <?php echo $facet_value['name'] ?> </h1>
<p class="lead">
<form method="POST" action="facet_value_edit.php">

    <input type="hidden" name="facet_value_id" value="<?php echo $facet_value['id'] ?>" />

    <div class="mb-3">
        <label for="facet_name" class="form-label">Facet value name</label>
        <input type="txt" class="form-control" id="name" name="name" aria-describedby="name_help"
            value="<?php echo $facet_value['name'] ?>" />
        <div id="name_help" class="form-text">Keep it short and meaningful.</div>
    </div>

    <div class="mb-3">
        <label for="description" class="form-label">Facet value description</label>
        <textarea class="form-control" id="description" name="description"
            aria-describedby="description_help"><?php echo $facet_value['description'] ?></textarea>
        <div id="description_help" class="form-text">A concise description of the facet value.</div>
    </div>

    <div class="mb-3">
        <label for="link_uri" class="form-label">Link URL</label>
        <input type="url" class="form-control" id="link_uri" name="link_uri" aria-describedby="link_uri_help"
            value="<?php echo $facet_value['link_uri'] ?>" />
        <div id="link_uri_help" class="form-text">A link to more information about the facet value.</div>
    </div>

    <div class="mb-3">
        <label for="link_uri" class="form-label">Code</label>
        <input type="txt" class="form-control" id="code" name="code" aria-describedby="link_uri_help"
            value="<?php echo $facet_value['code'] ?>" />
        <div id="link_uri_help" class="form-text">Used with this facet value is also part of a widely used vocabulary,
            like two letter ISO countries.</div>
    </div>


    <button class="btn btn-primary" type="button"
        onclick="document.location = '<?php echo "facet_values.php?facet_id={$facet['id']}#facet_value_{$facet_value['id']}"; ?>'">Cancel</button>
    &nbsp;
    <button type="submit" class="btn btn-primary">Save</button>
</form>
</p>

<?php
    require_once('footer.php');
?>