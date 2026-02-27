<?php

// Edit the properties of the source
if(@$_POST && @$_POST['properties_button'] &&  $user){

    $name_safe = $mysqli->real_escape_string($_POST['name']);
    $description_safe = $mysqli->real_escape_string($_POST['description']);
    $uri_safe = $mysqli->real_escape_string($_POST['link_uri']);
    $do_not_index = @$_POST['do_not_index'] && $_POST['do_not_index'] == 1 ? 1 : 0;

    $mysqli->query("UPDATE `sources` SET `name` = '$name_safe', `description` = '$description_safe', `link_uri` = '$uri_safe', `do_not_index` = $do_not_index WHERE id = $source_id;");

    $category_safe = $mysqli->real_escape_string($_POST['category']);
    $language_safe = $mysqli->real_escape_string($_POST['language']);
    $mysqli->query("UPDATE `snippet_sources` SET `category` = '{$category_safe}', `language` = '{$language_safe}'  WHERE source_id =  $source_id;");

    echo "<script>document.location = 'snippet_source.php?source_id={$source_id}&tab=properties-tab'</script>";

}

$name = $source['name'];
$description = $source['description'];
$link_uri = $source['link_uri'];
$category = $source['category'];
$language = $source['language'];
$do_not_index = @$source['do_not_index'] ? true : false;


?>
<p class="lead">
    Edit the metadata fields for this snippets data source.
</p>

<form method="POST" action="snippet_source.php">

    <input type="hidden" name="source_id" value="<?php echo $source_id ?>" />
    <input type="hidden" name="tab" value="properties-tab" />

    <div class="mb-3">
        <label for="name" class="form-label">Source name</label>
        <input type="txt" class="form-control" id="name" name="name" aria-describedby="name_help"
            value="<?php echo $name ?>" />
        <div id="name_help" class="form-text">Keep it short and meaningful but at least 8 characters long.</div>
    </div>

    <div class="mb-3">
        <label for="description" class="form-label">Source description</label>
        <textarea class="form-control" id="description" name="description"
            aria-describedby="description_help"><?php echo $description ?></textarea>
        <div id="description_help" class="form-text">A concise description of the source.</div>
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
        <label for="link_uri" class="form-label">Link URL</label>
        <input type="url" class="form-control" id="link_uri" name="link_uri" aria-describedby="link_uri_help"
            value="<?php echo $link_uri ?>" />
        <div id="link_uri_help" class="form-text">A link to more information about the source.</div>
    </div>


    <div class="mb-3 form-check">
        <label for="do_not_index" class="form-label">Do NOT index</label>
        <input type="checkbox" class="form-check-input" id="do_not_index" name="do_not_index" aria-describedby="do_not_index_help"
            value="1"
            <?php echo $do_not_index ? 'checked' : ''; ?> />
        <div id="do_not_index_help" class="form-text">Tick this box to prevent the datasource being index.
            This will not affect values already in the index if the source has previously been indexed.
            They will only change on the next complete re-index of all taxa.
        </div>
    </div>

    <button type="submit" name="properties_button" value="save" class="btn btn-primary">Save</button>
</form>