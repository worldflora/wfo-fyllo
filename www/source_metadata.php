<?php

/*

    ALTER TABLE fyllo.`sources` 
    ADD COLUMN `auto_import` TINYINT NOT NULL DEFAULT 1 AFTER `do_not_index`;

*/


// Edit the properties of the source
if($user && @$_POST && @$_POST['properties_button']){

    $name_safe = $mysqli->real_escape_string($_POST['name']);
    $description_safe = $mysqli->real_escape_string($_POST['description']);
    $uri_safe = $mysqli->real_escape_string($_POST['link_uri']);
    $path_safe = $mysqli->real_escape_string($_POST['github_path']);
    $do_not_index = @$_POST['do_not_index'] && $_POST['do_not_index'] == 1 ? 1 : 0;
    $auto_import = @$_POST['auto_import'] && $_POST['auto_import'] == 1 ? 1 : 0;

    // allow snippet fields to be null
    $category_safe = @$_POST['category'] ? "'" .$mysqli->real_escape_string($_POST['category']) . "'" : 'NULL';
    $language_safe = @$_POST['language'] ? "'" .$mysqli->real_escape_string($_POST['language']) . "'" : 'NULL';

    // we save the POST values to the session so that we can auto fill them
    $_SESSION['last_source_values'] = $_POST;

    // allow facet_value_id to be null for the snippet sources
    if(!$facet_value_id || $facet_value_id == 'snippet'){
        $facet_value_id_safe = 'NULL';
    }else{
        $facet_value_id_safe = $facet_value_id;
    }

    if($source_id){
        // we are updating
        $mysqli->query("UPDATE `sources` SET `name` = '$name_safe', `description` = '$description_safe', `link_uri` = '$uri_safe', `do_not_index` = $do_not_index, `auto_import` = $auto_import, `file_path` = '$path_safe', `snippet_language` = $language_safe, `snippet_category` = $category_safe WHERE id = $source_id;");
        echo '<div class="alert alert-success" role="alert">Source saved.</div>';
        echo "<script>window.location.href = \"source.php?source_id={$source_id}\"</script>";
    }else{

        // we are creating
        
        $mysqli->begin_transaction(); // as a transaction because two things need to happen at the same time

        try{
            $mysqli->query("INSERT INTO `sources` (`name`, `description`, `link_uri`, `do_not_index`, `auto_import`, `file_path`, `facet_value_id`,  `snippet_language`, `snippet_category` ) VALUES ('$name_safe', '$description_safe', '$uri_safe', $do_not_index, $auto_import, '$path_safe', $facet_value_id_safe, $language_safe, $category_safe)");
            $source_id = $mysqli->insert_id;
            $mysqli->commit();
            echo '<div class="alert alert-success" role="alert">Source "' . $_POST['name'] . '" created.</div>';
            echo "<script>window.location.href = \"source.php?source_id={$source_id}\"</script>";
        } catch (mysqli_sql_exception $exception) {
            $mysqli->rollback();
            echo '<div class="alert alert-danger" role="alert">Database insert failed.</div>';
            echo '<div class="alert alert-danger" role="alert">' . $mysqli->error . '</div>';
            print_r($exception);
        }

    }

}

$message = null;
if($source){
    $name = $source['name'];
    $description = $source['description'];
    $link_uri = $source['link_uri'];
    $do_not_index = $source['do_not_index'];
    $auto_import = $source['auto_import'];
    $file_path = $source['file_path'];
    $language = $source['snippet_language'];
    $category = $source['snippet_category'];
}else{

    // we prepopulate with the last saved values if they are in the session
    if( isset($_SESSION['last_source_values']) ){
        $name = $_SESSION['last_source_values']['name'];
        $description = $_SESSION['last_source_values']['description'];
        $link_uri = $_SESSION['last_source_values']['link_uri'];
        $do_not_index = isset($_SESSION['last_source_values']['do_not_index']) ? $_SESSION['last_source_values']['do_not_index'] : false;
        $auto_import = isset($_SESSION['last_source_values']['auto_import']) ? $_SESSION['last_source_values']['auto_import'] : false;
        $file_path = $_SESSION['last_source_values']['github_path'];
        $language = isset($_SESSION['last_source_values']['language']) ? $_SESSION['last_source_values']['language'] : 'zzz';
        $category = isset($_SESSION['last_source_values']['category']) ? $_SESSION['last_source_values']['category'] : 'general';
        $message = "Form populated from last saved source.";
    }else{
        $name = '';
        $description = '';
        $link_uri = '';
        $do_not_index = 0; // default to indexing
        $auto_import = 1; // default to auto importing
        $file_path = '';
        $language = 'zzz';
        $category = 'general';
    }


}

if($user){
    // they are logged in so render a form
    
    $disabled = '';

    if(!$source){
        if($facet_value){
            echo "<p class=\"lead\">This is a new source for <strong>{$facet_value['facet_name']}: {$facet_value['facet_value_name']}</strong>.</p>";
        }else{
            echo "<p class=\"lead\">This is a new source of snippet data.</p>";
        }
    }else{
        // we are editing an existing one
        echo '<p class="lead">Edit the metadata fields for this data source.</p>';
    }
}else{
   $disabled = 'disabled'; // flag to disable all the fields
   echo '<div class="alert alert-success" role="alert">Read only metadata view.</div>';
}

if($message){
    echo '<div class="alert alert-success" role="alert">'. $message .'</div>'; 
}

?>

<form method="POST" action="source.php">

    <input type="hidden" name="source_id" value="<?php echo $source_id ?>" />
    <input type="hidden" name="facet_value_id" value="<?php echo $facet_value_id ?>" />

    <input type="hidden" name="tab" value="properties-tab" />

    <div class="mb-3">
        <label for="name" class="form-label">Source name </label>
        <input type="txt" class="form-control" id="name" name="name" aria-describedby="name_help"
            value="<?php echo $name ?>" <?php echo $disabled  ?> />
        <div id="name_help" class="form-text">Keep it short and meaningful but at least 8 characters long.</div>
    </div>

    <div class="mb-3">
        <label for="description" class="form-label">Source description</label>
        <textarea class="form-control" id="description" name="description"
            aria-describedby="description_help" <?php echo $disabled  ?> rows="12" ><?php echo $description ?></textarea>
        <div id="description_help" class="form-text">A concise description of the source.</div>
    </div>

    <div class="mb-3">
        <label for="link_uri" class="form-label">Link URL</label>
        <input type="url" class="form-control" id="link_uri" name="link_uri" aria-describedby="link_uri_help"
            value="<?php echo $link_uri ?>" <?php echo $disabled  ?> />
        <div id="link_uri_help" class="form-text">A link to more information about the source.</div>
    </div>
<?php if($snippet_mode){ ?>
<!-- CATEGORY -->

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
    $result = $mysqli->query("SHOW COLUMNS FROM `sources` LIKE 'snippet_category'");
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


<!-- LANGUAGE -->
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
    $result = $mysqli->query("SHOW COLUMNS FROM `sources` LIKE 'snippet_language'");
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
<?php } ?>
    <div class="mb-3">
        <div class="input-group">
            <input id="github-file-path" name="github_path" value="<?php echo $file_path ?>" type="text" class="form-control" readonly placeholder="Path within GitHub repository" aria-label="Path within GitHub repository" aria-describedby="pick-button-help">
            <button class="btn btn-outline-secondary" type="button" data-bs-toggle="modal" id="pick-button" data-bs-target="#dsFilePicker" <?php echo $disabled  ?>>DS Picker</button>
        </div>
        <div id="pick-button-help" class="form-text">Use the picker to select a CSV file from the GitHub repository. You can't enter this path manually.</div>
    </div>

    <!-- Auto Import -->
    <div class="mb-3 form-check">
        <label for="auto_import" class="form-label">Auto Import</label>
        <input type="checkbox" class="form-check-input" id="auto_import" name="auto_import" aria-describedby="auto_import_help"
            value="1"
            <?php echo $auto_import ? 'checked' : ''; ?> <?php echo $disabled  ?>/>
        <div id="auto_import" class="form-text">If this box is ticked then an import script (run by the Airflow orchestrator) will
            periodically keep it up to date with any changes made to the linked GitHub file. 
        </div>
    </div>


    <!-- Do NOT index -->
    <div class="mb-3 form-check">
        <label for="do_not_index" class="form-label">Do NOT index</label>
        <input type="checkbox" class="form-check-input" id="do_not_index" name="do_not_index" aria-describedby="do_not_index_help"
            value="1"
            <?php echo $do_not_index ? 'checked' : ''; ?> <?php echo $disabled  ?>/>
        <div id="do_not_index_help" class="form-text">Tick this box to prevent the datasource being indexed.
            This will not affect values already in the index if the source has previously been indexed.
            They will only change on the next complete re-index of all taxa.
        </div>
    </div>

    <div class="mb-3" style="text-align: right">
        <a type="button" href="source.php?source_id=<?php echo $source_id ?>" name="properties_button" value="save" class="btn btn-outline-secondary" <?php echo $disabled  ?> >Cancel</a>
        &nbsp;
        <button type="submit" name="properties_button" value="save" class="btn btn-primary" <?php echo $disabled  ?>>Save</button>
    </div>

</form>


<!-- Modal for picking a file -->
<div class="modal fade" id="dsFilePicker" tabindex="-1" aria-labelledby="dsFilePicker" aria-hidden="true">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header">
        <h1 class="modal-title fs-5" id="dsFilePicker">Data Source File Picker</h1>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div id="dsFilePickerContent">
        Pick you file here...
      </div>
    </div>
  </div>
</div>

<script>
// modal dialogue - load content on show

document.getElementById('dsFilePicker').addEventListener('show.bs.modal', event => {
    update_ds_pick_list();
})

function update_ds_pick_list(path = ''){

    const modalContent = document.getElementById('dsFilePickerContent');
    modalContent.innerHTML = 'Loading ...';
    fetch("data_source_file_picker.php?path=" + path)
        .then(response => response.text())
        .then(text => {
            modalContent.innerHTML = text;
            // add event listeners to our folder elements
            const folders = document.getElementsByClassName('wfo-ds-file-pick-folder');
            for (var i=0; i < folders.length; i++) {
              folders[i].onclick = function(event){
                console.log(event.target.dataset.wfoPath);
                // check if this is a file or not
                update_ds_pick_list(event.target.dataset.wfoPath);
              }
            };

            // add event listeners to our pickable files
            const files = document.getElementsByClassName('wfo-ds-file-pickable');
            for (var i=0; i < files.length; i++) {
              files[i].onclick = function(event){
                document.getElementById('github-file-path').value = event.target.dataset.wfoPath;
                // check if this is a file or not
                console.log("You picked me!");

                bootstrap.Modal.getInstance(document.getElementById("dsFilePicker")).hide();
                document.getElementById('github-file-path').focus();
              }
            };


        });// has text

}

</script>

