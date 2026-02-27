<?php

require_once('../config.php');
require_once('../include/FileStore.php');


$store = new FileStore(@$_GET['path']);
$folder_icon_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-folder" viewBox="0 0 16 16">
  <path d="M.54 3.87.5 3a2 2 0 0 1 2-2h3.672a2 2 0 0 1 1.414.586l.828.828A2 2 0 0 0 9.828 3h3.982a2 2 0 0 1 1.992 2.181l-.637 7A2 2 0 0 1 13.174 14H2.826a2 2 0 0 1-1.991-1.819l-.637-7a2 2 0 0 1 .342-1.31zM2.19 4a1 1 0 0 0-.996 1.09l.637 7a1 1 0 0 0 .995.91h10.348a1 1 0 0 0 .995-.91l.637-7A1 1 0 0 0 13.81 4zm4.69-1.707A1 1 0 0 0 6.172 2H2.5a1 1 0 0 0-1 .981l.006.139q.323-.119.684-.12h5.396z"/>
</svg>';
$file_icon_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-file-earmark" viewBox="0 0 16 16">
  <path d="M14 4.5V14a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V2a2 2 0 0 1 2-2h5.5zm-3 0A1.5 1.5 0 0 1 9.5 3V1H4a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V4.5z"/>
</svg>';

if($store->entries){
    // we are a directory and need to display content
    echo '<div class="list-group list-group-flush" style="max-height: 30em; overflow: auto;">';
    foreach ($store->entries as $entry) {
        if($entry->type == 'tree'){
            // flagged with a class so we can add an event listener to it
            $colour = $entry->name == 'out' ? 'style="color: green"; font-weight: bold;' : '';
            echo "<a href=\"#\" class=\"list-group-item list-group-item-action wfo-ds-file-pick-folder\" {$colour} data-wfo-path=\"{$entry->path}\">{$folder_icon_svg}&nbsp;&nbsp;{$entry->name}</a>";
        }else{
            // either disabled or flagged so we can add an event listener to it
            $disabled = $entry->pickable ? 'wfo-ds-file-pickable' : 'disabled';
            echo "<a href=\"#\" class=\"list-group-item list-group-item-action {$disabled}\" data-wfo-path=\"{$entry->path}\" >{$file_icon_svg}&nbsp;&nbsp;{$entry->name} </a>";
        }
    }
    echo "</div>";
}else{
    // we are a file and need to display that
    print_r($store->file);
}

?>
