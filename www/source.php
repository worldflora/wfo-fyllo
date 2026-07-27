<?php
    require_once('../include/language_codes.php');
    require_once('header.php');
    require_once('../include/Parsedown.php');

    $source_id = (int)@$_REQUEST['source_id']; // will be null when creating
    $facet_value_id = @$_REQUEST['facet_value_id']; // may be null when we are editing 
    $source = null; // won't be set if we are creating

    if($source_id && !$facet_value_id){
        // render the header as if we are editing an existing source
        $response = $mysqli->query("SELECT * FROM `sources` as s WHERE id = $source_id");
        $source = $response->fetch_assoc();
        $response->close();
        $facet_value_id = $source['facet_value_id'];
    }

    // now we are sure to have a facet_value_id we can load the facet info 
    // load up the facet value we are looking at
    if(!$facet_value_id  || $facet_value_id == 'snippet'){
        $snippet_mode = true;
        $facet_value = null;
        $facet_value_id == null;
        
        // crumb trail
        if(isset($source['id'])){
            $language_display = $language_codes_alpha3[$source['snippet_language']]['eng'];
            $category_display = ucfirst($source['snippet_category']);
            echo "<p><a href=\"facets.php\">Fyllo</a> 
                → <a href=\"snippets.php\">Snippets</a>
                → <a href=\"snippets.php?category={$source['snippet_category']}\">{$category_display}</a> 
                → <a href=\"snippets.php?language={$source['snippet_language']}&category={$source['snippet_category']}\">{$language_display}</a> 
                → Data Source: {$source['id']}
            </p>";
        }else{
            echo "<p><a href=\"facets.php\">Fyllo</a> 
                → <a href=\"snippets.php\">Snippets</a>
                → Data Source: New</p>";

        }

    }else{
        $snippet_mode = false;
        $response = $mysqli->query("SELECT 
            f.id as facet_id, fv.id as facet_value_id, f.name as facet_name, fv.name as facet_value_name 
            FROM facet_values as fv 
            JOIN facets as f on fv.facet_id = f.id 
            WHERE fv.id = {$facet_value_id};");
        $facet_value = $response->fetch_assoc();
        $response->close();
        
        // crumb trail
        $source_id_display = isset($source['id']) ? $source['id'] : 'New';
        echo "<p><a href=\"facets.php\">Fyllo</a> 
            → <a href=\"facets.php\">Facets</a> 
            → <a href=\"facet_values.php?facet_id={$facet_value['facet_id']}\">{$facet_value['facet_name']}</a> 
            → <a href=\"facet_values.php?facet_id={$facet_value['facet_id']}\">{$facet_value['facet_value_name']}</a> 
            → Data Source: {$source_id_display}
        </p>";
        //echo "<p><a href=\"facet_values.php?facet_id={$facet_value['facet_id']}\">{$facet_value['facet_name']}: {$facet_value['facet_value_name']}</a>.</p>";
   
    }
 
    echo '<div style="float: right;">';
    if($user && $source){
        echo '&nbsp;<a class="btn btn-sm btn-outline-danger" href="#" onclick="alert(\'Deleting is serious business and is currently done at the database level only.\')" role="button">Delete source</a>';
    }
    echo '</div>';

  
    if($source){
        echo "<h1>{$source['name']}</h1>";
        $parser = new Parsedown();
        $source_html = $parser->text($source['description']);
        echo "<div class=\"lead\">{$source_html}</div>";
    }else{
        if($facet_value_id == 'snippet'){
            echo "<h1>Creating a snippet data source</h1>";
        }else{
            echo "<h1>Creating a facet data source</h1>";
        }
        
        echo "<p class=\"lead\">Enter the metadata below.</p>";
    }
 
    // get the source


?>

<ul class="nav nav-tabs" id="myTab" role="tablist" style="margin-bottom: 1em;">

    <li class="nav-item" role="presentation">
        <button class="nav-link  <?php echo !@$_REQUEST['tab'] ? 'active' : '' ?>" id="properties-tab" data-bs-toggle="tab" data-bs-target="#metadata" type="button"
            role="tab">Metadata</button>
    </li>

<?php if($source){ ?>
    <li class="nav-item" role="presentation">
        <button class="nav-link <?php echo @$_REQUEST['tab'] == 'import' ? 'active' : '' ?>" id="import-tab" data-bs-toggle="tab" data-bs-target="#import" type="button"
            role="tab">Import</button>
    </li>

    <li class="nav-item" role="presentation">
        <button class="nav-link <?php echo @$_REQUEST['tab'] == 'metadata' ? 'active' : '' ?>" id="list-tab" data-bs-toggle="tab" data-bs-target="#list" type="button"
            role="tab">List</button>
    </li>
<?php } // if source ?>
</ul>


<div class="tab-content" id="myTabContent">
    
    <!-- METADATA -->
    <div class="tab-pane fade <?php echo !@$_REQUEST['tab'] ? 'show active' : '' ?>" id="metadata" role="tabpanel">
        <?php require_once('source_metadata.php'); ?>
    </div>


<?php if($source){ ?>
    <!-- IMPORT DISPLAY -->
    <div class="tab-pane fade <?php echo @$_REQUEST['tab'] == 'import' ? 'show active' : '' ?>" id="import" role="tabpanel" aria-labelledby="import-tab">
        <?php require_once('source_import.php'); ?>
    </div>

    <!-- LIST DISPLAY -->
    <div class="tab-pane fade <?php echo @$_REQUEST['tab'] == 'list' ? 'show active' : '' ?>" id="list" role="tabpanel" aria-labelledby="list-tab">
        <?php require_once('source_list.php'); ?>
    </div>
<?php } // if source ?>
</div>



<?php
    require_once('footer.php');
?>

<script>
<?php
    // we need to be able to display a particular tab.
    if(@$_REQUEST['tab']){
        echo "var someTabTriggerEl = document.querySelector('#{$_REQUEST['tab']}');\n";
        echo "var tab = new bootstrap.Tab(someTabTriggerEl);\n";
        echo "tab.show();\n";
    }
?>

const listTab = document.getElementById('list-tab');
listTab.addEventListener('shown.bs.tab', event => {
    // listChanged is set to false when the page
    // is loaded then if any javascript process changes the list
    // it sets it to true so that clicking on the list tab 
    // will refresh the page
    if (listChanged) {
        document.location = 'source.php?tab=list-tab&source_id=<?php echo $source_id ?>'
    }
});
</script>