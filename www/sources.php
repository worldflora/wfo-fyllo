<?php
    require_once('header.php');
    $tags_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-tags" viewBox="0 0 16 16">
  <path d="M3 2v4.586l7 7L14.586 9l-7-7zM2 2a1 1 0 0 1 1-1h4.586a1 1 0 0 1 .707.293l7 7a1 1 0 0 1 0 1.414l-4.586 4.586a1 1 0 0 1-1.414 0l-7-7A1 1 0 0 1 2 6.586z"/>
  <path d="M5.5 5a.5.5 0 1 1 0-1 .5.5 0 0 1 0 1m0 1a1.5 1.5 0 1 0 0-3 1.5 1.5 0 0 0 0 3M1 7.086a1 1 0 0 0 .293.707L8.75 15.25l-.043.043a1 1 0 0 1-1.414 0l-7-7A1 1 0 0 1 0 7.586V3a1 1 0 0 1 1-1z"/>
</svg>';
    $text_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-card-text" viewBox="0 0 16 16">
  <path d="M14.5 3a.5.5 0 0 1 .5.5v9a.5.5 0 0 1-.5.5h-13a.5.5 0 0 1-.5-.5v-9a.5.5 0 0 1 .5-.5zm-13-1A1.5 1.5 0 0 0 0 3.5v9A1.5 1.5 0 0 0 1.5 14h13a1.5 1.5 0 0 0 1.5-1.5v-9A1.5 1.5 0 0 0 14.5 2z"/>
  <path d="M3 5.5a.5.5 0 0 1 .5-.5h9a.5.5 0 0 1 0 1h-9a.5.5 0 0 1-.5-.5M3 8a.5.5 0 0 1 .5-.5h9a.5.5 0 0 1 0 1h-9A.5.5 0 0 1 3 8m0 2.5a.5.5 0 0 1 .5-.5h6a.5.5 0 0 1 0 1h-6a.5.5 0 0 1-.5-.5"/>
</svg>';

    $page = 0;
    if(isset($_GET['current_page'])) $page = (int)$_GET['current_page']; // if the current page hasn't changed
    if(isset($_GET['page'])) $page = (int)$_GET['page']; // if we move to a new page    
    $page_size = 10;
    $offset = $page * $page_size;

    $sort_order = 'name';
    if(@$_GET['sort_order']) $sort_order = @$_GET['sort_order'];

    switch ($sort_order) {
        case 'name':
            $where = '';
            $order_by = 'ORDER BY `name` ASC';
            break;
        case 'import_desc':
            $where = 'WHERE `last_import` IS NOT NULL';
            $order_by = 'ORDER BY `last_import` DESC';
            break;
        case 'import_asc':
            $where = 'WHERE `last_import` IS NOT NULL';
            $order_by = 'ORDER BY `last_import` ASC';
            break;
        case 'import_never':
            $where = 'WHERE `last_import` IS NULL';
            $order_by = 'ORDER BY `name` ASC';
            break;
        default:
            $where = '';
            $order_by = 'ORDER BY `name`';
            break;
    }

    $search_term = '';
    $search_term = null;
    if(isset($_GET['search_term'])){
        $search_term = $_GET['search_term'];
        $search_term_safe = $mysqli->real_escape_string($search_term);
        if($where) $where .= " AND ";
        else $where .= " WHERE ";
        $where .= " name LIKE '%$search_term_safe%'";
    } 


    
    if($user){
        echo '<div style="float: right;">';
        // kept for spacing
        echo '</div>';
    } 

    // get the basic stats
    

    $response = $mysqli->query("SELECT count(*) as total, count(last_import) as imported from sources;");
    $stats = $response->fetch_assoc();
    $response->close();

?>
<script>
    function update_sort(sort_type){
        const so = document.getElementById('sort_order');
        so.value= sort_type;
        const cp = document.getElementById('current_page');
        cp.value= 0;
        cp.form.submit();
        console.log(sort_type);
    }
</script>
<form method="GET" action="sources.php">
    <input type="hidden" name="current_page" id="current_page" value="<?php echo $page ?>" />

    <p><a href="index.php">Fyllo</a> → Sources</p>
    <h1>Sources</h1>
    <p class="lead">
        These are the <strong><?php echo number_format($stats['total'], 0) ?></strong> sources in the system, both for facet values and snippets.
        <strong><?php echo number_format($stats['imported'], 0) ?></strong> of these have been imported.
    </p>
    <ul class="list-group">
        <li class="list-group-item">
            <div class="row">
                <div class="col col-12 input-group">
                    <input type="hidden" name="sort_order" id="sort_order" value="<?php echo $sort_order ?>" />
                    <input type="text" name="search_term" value="<?php echo $search_term; ?>" class="form-control" placeholder="Name contains" aria-label="Name contains" />
                    <button class="btn btn-outline-secondary" type="submit">Search</button>
                    <button type="button" class="btn btn-outline-secondary dropdown-toggle dropdown-toggle-split"
                        data-bs-toggle="dropdown" aria-expanded="false">
                        <span>Sort</span>
                    </button>
                    <ul class="dropdown-menu dropdown-menu-end">
                        <li><a class="dropdown-item" href="#" onclick="update_sort('name');">By name</a></li>
                        <li><a class="dropdown-item" href="#" onclick="update_sort('import_desc');">Most recent imports</a></li>
                        <li><a class="dropdown-item" href="#" onclick="update_sort('import_asc');">Oldest imports</a></li>
                        <li><a class="dropdown-item" href="#" onclick="update_sort('import_never');">Never imported</a></li>
                    </ul>
                </div>
            </div>

            <?php
    $response = $mysqli->query("SELECT * FROM `sources` as s $where $order_by LIMIT $page_size OFFSET $offset;");
    $sources = $response->fetch_all(MYSQLI_ASSOC);
    $response->close();
    
    foreach($sources as $s){

        // load up the facet value we are looking at
        echo '<li class="list-group-item">';
        echo '<div class="row">';

        echo '<div class="col col-10">';

        // icon to show source kind
        echo $s['facet_value_id'] ? $tags_svg :  $text_svg;

        // source name linked to source
        echo "&nbsp;&nbsp;<a href=\"source.php?source_id={$s['id']}\">{$s['name']}</a>";

        // details
        if($s['facet_value_id']){
            // this is a facet value data source
            $response = $mysqli->query("SELECT f.id as facet_id, fv.id as facet_value_id, f.name as facet_name, fv.name as facet_value_name FROM facet_values as fv JOIN facets as f on fv.facet_id = f.id WHERE fv.id = {$s['facet_value_id']}");
            $facet_value = $response->fetch_assoc();
            $response->close();
            echo "<br/><strong>Facet:</strong> <a href=\"facet_values.php?facet_id={$facet_value['facet_id']}\">{$facet_value['facet_name']}</a>";
            echo " <strong>Value:</strong> <a href=\"facet_values.php?facet_id={$facet_value['facet_id']}#fv{$facet_value['facet_value_id']}\">{$facet_value['facet_value_name']}</a>";
        }else{
            // this is a snippet data source
            echo "<br/><strong>Category:</strong> <a href=\"snippets.php?category={$s['snippet_category']}\">{$s['snippet_category']}</a>";
            echo " <strong>Language:</strong> <a href=\"snippets.php?language={$s['snippet_language']}\">{$s['snippet_language']}</a>";
        }
        echo '</div>'; // end of col
        echo '<div class="col col-2" style="text-align: right;">';
        echo "{$s['last_import']}";
        echo '</div>'; // end of col
        echo '</div>'; // end of row
        echo '</li>';



    }

    // count things up for the paging
    $response = $mysqli->query("SELECT count(*) as n FROM `sources` as s $where ;");
    $row = $response->fetch_assoc();
    $n = $row['n'];
    $response->close();   

    $total_pages = floor($n/$page_size) +1;

    $previous_page = $page - 10;
    if($previous_page < 0) $previous_page = 0;

    $next_page = $page + 10;
    if($next_page > $total_pages) $next_page = $total_pages;

?>
        <li class="list-group-item d-flex justify-content-center">
            <nav aria-label="Page navigation example">
                <ul class="pagination">
                    <li class="page-item">
                        <button class="page-link btn" type="submit" aria-hidden="true" aria-label="Previous" name="page"
                            value="<?php echo $previous_page ?>"
                            <?php echo $previous_page == $page ? 'disabled' : '' ?>>&laquo;</button>
                    </li>
        </li>

        <?php

    $start = $page < 10 ? 0 : $page - $page % 10;
    $end = $start + 10;
    if($end > $total_pages) $end = $total_pages;

    for ($i=$start ; $i < $end; $i++ ) { 
        echo ' <li class="page-item">';
        $disabled = $i == $page ? 'disabled btn-secondary' : '';
        echo "<button class=\"page-link btn $disabled \" type=\"submit\" name=\"page\" value=\"{$i}\" >";
        echo $i + 1;
        echo '</button>';
        echo '</li>';
    }
?>
        <li class="page-item">
            <button class="page-link btn" type="submit" aria-hidden="true" aria-label="Next" name="page"
                value="<?php echo $next_page ?>" <?php echo $next_page == $page ? 'disabled' : '' ?>>&raquo;</button>
        </li>
        </li>
    </ul>
    </nav>
    </li>
    </ul>

</form>
<?php
    require_once('footer.php');
?>