<?php
    require_once('header.php');

    // You must be logged in or there must be no users - prevent sawing off our own branch
    $response = $mysqli->query("SELECT count(*) as n FROM `users`");
    $row = $response->fetch_assoc();
    $user_count = $row['n'];

    if(!$user && $user_count > 0){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }

?>
<h1>Users</h1>
<p class="lead">
    These are the users registered with the system. There are no roles or anything complex. Anyone who logs in has rights to do anything that can be done.
    There are no password recovery facilities. You must ask another user to change your password for you.
</p>
<ul class="list-group">
    <?php
    $response = $mysqli->query("SELECT * FROM `users` ORDER BY `username`;");
    while($u = $response->fetch_assoc()){
        echo '<li class="list-group-item">';
        echo '<div class="row">';
        
        echo '<div class="col">';
        echo "<strong>{$u['username']}</strong>";
        echo '</div>'; // end of col

        // if they are god then show the edit buttons
        
        echo '<div class="col" style="text-align: right;">';
            echo '&nbsp;<a class="btn btn-sm btn-outline-secondary" href="user_password_reset.php?username='.$u['username'].'&user_id='. $u['id'] .'" role="button">Reset password</a>';
            echo '&nbsp;<a class="btn btn-sm btn-outline-danger" href="user_delete.php?user_id='. $u['id'] .'" role="button">Delete</a>';
        echo '</div>'; // end of col
        echo '</div>'; // end of row
        echo '</li>';
    }

    echo '<li class="list-group-item" style="text-align: right;">';

    echo '<button class="btn btn-sm btn-outline-secondary" onclick="localStorage.clear();" >Clear Cache</button>&nbsp;';
    
    echo '<a class="btn btn-sm btn-success" href="user_create.php" role="button">Add user</a>';
    echo '</li>';

?>
</ul>
<?php
    require_once('footer.php');
?>