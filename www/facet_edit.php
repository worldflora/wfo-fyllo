<?php
    require_once('header.php');


    if(!$user){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }

    $facet_id = (int)@$_REQUEST['facet_id'];
    
    if($_POST){

        $name = trim(@$_POST['name']);
        $description = @$_POST['description'];
        $link_uri = @$_POST['link_uri'];
        $heritable = @$_POST['heritable_checkbox'];

        print_r($_POST);

        // check username string OK
        if(strlen($name) > 7){
            

            $name_safe = $mysqli->real_escape_string($name);
            $description_safe = $mysqli->real_escape_string($description);
            $link_uri_safe = $mysqli->real_escape_string($link_uri);
            $heritable_safe = $heritable ? 1 : 0;

            $sql = "UPDATE `facets` SET `name` = '$name_safe', `description` = '$description_safe', `link_uri` = '$link_uri_safe', `heritable` = $heritable_safe WHERE id = $facet_id;";
            $mysqli->query($sql);

            if($mysqli->error){
                echo '<div class="alert alert-danger" role="alert">Database update failed.</div>';
                echo '<div class="alert alert-danger" role="alert">' . $mysqli->error . '</div>';
            }else{
                echo '<div class="alert alert-success" role="alert">Facet "' . $name . '" saved.</div>';
                echo "<script>window.location.href = \"facet_values.php?facet_id=$facet_id\"</script>";
            }
                
        }else{
            echo '<div class="alert alert-danger" role="alert">The name must be greater than 7 characters long.</div>';
        }
    }else{

        // we are GET and so loading
        $response = $mysqli->query("SELECT * FROM `facets` WHERE id = $facet_id");
        $facet = $response->fetch_assoc();
        $response->close();


        $name = $facet['name'];
        $description = $facet['description'];
        $link_uri = $facet['link_uri'];
        $heritable = $facet['heritable'];
    }

    $heritable_checked = $heritable ? 'checked' : '';
?>

<h1>Edit facet</h1>
<p class="lead">
<form method="POST" action="facet_edit.php">

    <input type="hidden" name="facet_id" value="<?php echo $facet_id ?>" />

    <div class="mb-3">
        <label for="facet_name" class="form-label">Facet name</label>
        <input type="txt" class="form-control" id="name" name="name" aria-describedby="name_help"
            value="<?php echo $name ?>" />
        <div id="name_help" class="form-text">Keep it short and meaningful.</div>
    </div>

    <div class="mb-3">
        <label for="description" class="form-label">Facet description (<a href="https://en.wikipedia.org/wiki/Markdown" target="manual">MarkDown</a>)</label>
        <textarea class="form-control" id="description" name="description"
            aria-describedby="description_help"><?php echo $description ?></textarea>
        <div id="description_help" class="form-text">A concise description of the facet.</div>
    </div>

    <div class="mb-3">
        <label for="link_uri" class="form-label">Link URL</label>
        <input type="txt" class="form-control" id="link_uri" name="link_uri" aria-describedby="link_uri_help"
            value="<?php echo $link_uri ?>" />
        <div id="link_uri_help" class="form-text">A link to more information about the facet.</div>
    </div>

    <div class="mb-3" >
        <label for="heritable_checkbox" class="form-label">Heritable: </label>
        <input type="checkbox" class="form-check-input" id="heritable_checkbox" name="heritable_checkbox" aria-describedby="heritable_checkbox_help" value="1"  <?php echo $heritable_checked ?> />
        <div id="heritable_checkbox_help" class="form-text">Are the facet values inherited from parent taxon to child? e.g. Check the box if an attribute of the species should also be applied to its subspecies.</div>
    </div>

    <button type="submit" class="btn btn-primary">Save</button>
</form>
</p>

<?php
    require_once('footer.php');
?>