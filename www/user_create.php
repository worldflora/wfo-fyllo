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

    if($_POST){

        $username = $_POST['username'];
        $password = trim($_POST['password']);

        // check username string OK
        if(preg_match('/^[a-zA-Z0-9]+$/',$username)){
            if(strlen($password) > 7){
                
                $password_hash = password_hash($password, PASSWORD_DEFAULT);
                $mysqli->query("INSERT INTO `users` (`username`, `password_hash`) VALUES ('$username', '$password_hash');");
                if($mysqli->error){
                    echo '<div class="alert alert-danger" role="alert">Database insert failed. Does the user already exist? </div>';
                }else{
                    echo '<div class="alert alert-success" role="alert">User "' . $username . '" created.</div>';
                }
                
            }else{
                echo '<div class="alert alert-danger" role="alert">Password too short.</div>';
            }
        }else{
            echo '<div class="alert alert-danger" role="alert">Username contains more than letters and numbers.</div>';
        }


    }

?>

<p><a href="index.php">Fyllo</a> → <a href="users.php">Users</a>  → Create</p>
<h1>Create User</h1>
<p class="lead">
<form method="POST" action="user_create.php">
    <div class="mb-3">
        <label for="username" class="form-label">Username</label>
        <input type="txt" class="form-control" id="username" name="username" aria-describedby="username_help"
            value="<?php echo @$username ?>" />
        <div id="username_help" class="form-text">Keep it short and meaningful. Characters and numbers only.</div>
    </div>
    <div class="mb-3">
        <label for="password" class="form-label">Password</label>
        <input type="text" class="form-control" id="password" name="password" aria-describedby="password_help">
        <div id="password_help" class="form-text">Make it complex, minimum 8 characters. <strong>Make a note of it. You
                won't see it again!</strong></div>
    </div>
    <button type="submit" class="btn btn-primary">Create</button>
</form>
</p>

<?php
    require_once('footer.php');
?>