<?php
    require_once('header.php');
    require_once('../include/language_codes.php');

    // nobody but the gods
    if(!$user){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }


    // get the params
    $name = trim(@$_REQUEST['name']);
    $description = @$_REQUEST['description'];
    $link_uri = @$_REQUEST['link_uri'];
    $language = @$_REQUEST['language'];
    $category = @$_REQUEST['category'];

    if(!$category) $category = 'general';
    if(!$language) $language = 'en';

    if($_POST){

        if(strlen($name) > 7){

            $name_safe = $mysqli->real_escape_string($name);
            $description_safe = $mysqli->real_escape_string($description);
            $link_uri_safe = $mysqli->real_escape_string($link_uri);
            $category_safe = $mysqli->real_escape_string($category);
            $language_safe = $mysqli->real_escape_string($language);

            $mysqli->begin_transaction();

            try{
                $mysqli->query("INSERT INTO `sources` (`name`, `description`, `link_uri`) VALUES ('$name_safe', '$description_safe', '$link_uri_safe');");
                $source_id = $mysqli->insert_id;
                $mysqli->query("INSERT INTO `snippet_sources` (`source_id`, `category`, `language`) VALUES ( $source_id, '$category_safe', '$language_safe');");
                $mysqli->commit();
                
                echo '<div class="alert alert-success" role="alert">Snippet "' . $name . '" created.</div>';
                echo "<script>window.location.href = \"snippet_source.php?source_id={$source_id}\"</script>";

            } catch (mysqli_sql_exception $exception) {
                $mysqli->rollback();
                echo '<div class="alert alert-danger" role="alert">Database insert failed.</div>';
                echo '<div class="alert alert-danger" role="alert">' . $mysqli->error . '</div>';
                print_r($exception);
            }
                
        }else{
            echo '<div class="alert alert-danger" role="alert">The name must be greater than 7 characters long.</div>';
        }
    }

?>

<h1>Create a new snippet source</h1>
<p class="lead">
    
</p>
<form method="POST" action="snippet_source_create.php">

    <div class="mb-3">
        <label for="name" class="form-label">Source name</label>
        <input type="txt" class="form-control" id="name" name="name" aria-describedby="name_help"
            value="<?php echo $name ?>" />
        <div id="name_help" class="form-text">Keep it short and meaningful but at least 8 characters long.</div>
    </div>

    <div class="mb-3">
        <label for="name" class="form-label">Category</label>
        <select
            class="form-select"
            id="category" 
            name="category" 
            aria-describedby="category_help"
            >

<?php

    // write in the category choices
    $result = $mysqli->query("SHOW COLUMNS FROM `snippet_sources` LIKE 'category'");
    $row = $result->fetch_assoc();
    $result->close();

    $type = $row['Type'];
    preg_match("/'(.*)'/i", $type, $matches);
    $vals = explode(',', $matches[1]);
    array_walk($vals, function(&$v){$v = str_replace("'", "", $v);});
    sort($vals);
    foreach($vals as $val){
        // default to general as we are creating
        $selected = $val == $category ? 'selected' : '';
        echo "<option value=\"{$val}\" {$selected}>$val</val>";
    }

?>

        </select>
        <div id="category_help" class="form-text">You must to specify the category of text snippets in this data source.</div>
       
    </div>

    <div class="mb-3">
        <label for="name" class="form-label">Language</label>
        <select
            class="form-select"
            id="language" 
            name="language" 
            aria-describedby="language_help"
            >

<?php

    // language choices
    $result = $mysqli->query("SHOW COLUMNS FROM `snippet_sources` LIKE 'language'");
    $row = $result->fetch_assoc();
    $result->close();

    $type = $row['Type'];
    preg_match("/'(.*)'/i", $type, $matches);
    $vals = explode(',', $matches[1]);
    array_walk($vals, function(&$v){$v = str_replace("'", "", $v);});
    sort($vals);
    foreach($vals as $val){
        // default to general as we are creating
        $selected = $val == $language ? 'selected' : '';
        $language_name = "{$language_codes_alpha3[$val]['eng']} ({$val})";
        echo "<option value=\"{$val}\" {$selected}>{$language_name}</val>";
    }

?>

        </select>
        <div id="category_help" class="form-text">You must to specify the language of the text snippets in this data source.</div>
       
    </div>


    <div class="mb-3">
        <label for="description" class="form-label">Source description</label>
        <textarea class="form-control" id="description" name="description"
            aria-describedby="description_help"><?php echo $description ?></textarea>
        <div id="description_help" class="form-text">A concise description of the source.</div>
    </div>

    <div class="mb-3">
        <label for="link_uri" class="form-label">Link URL</label>
        <input type="url" class="form-control" id="link_uri" name="link_uri" aria-describedby="link_uri_help"
            value="<?php echo $link_uri ?>" />
        <div id="link_uri_help" class="form-text">A link to more information about the source.</div>
    </div>

    <button type="submit" class="btn btn-primary">Create</button>
</form>


<?php
    require_once('footer.php');
?>