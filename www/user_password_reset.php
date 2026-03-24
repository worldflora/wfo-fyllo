<?php
    require_once('header.php');

    // nobody but the gods
    if(!$user){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }

    $username = $_REQUEST['username'];
    $user_id = (int)$_REQUEST['user_id'];

    // they are submitting the form
    if($_POST){

        $password = trim($_POST['password']);

        if(strlen($password) > 7){
            
            $password_hash = password_hash($password, PASSWORD_DEFAULT);
            $mysqli->query("UPDATE `users` SET `password_hash` = '$password_hash' WHERE id = $user_id;");
            if($mysqli->error){
                echo '<div class="alert alert-danger" role="alert">Database insert failed.</div>';
                echo '<div class="alert alert-danger" role="alert">' . $mysqli->error . '</div>';
            }else{
                echo '<div class="alert alert-success" role="alert">Password updated for '. $username . '.</div>';
            }
            
        }else{
            echo '<div class="alert alert-danger" role="alert">Password too short.</div>';
        }

    }

?>

<p><a href="index.php">Fyllo</a> → <a href="users.php">Users</a>  → Password</p>
<h1>Reset Password for '<?php echo $username; ?>'</h1>
<p class="lead">
<form method="POST" action="user_password_reset.php">
    <input type="hidden" name="user_id" value="<?php echo $user_id ?>" />
    <input type="hidden" name="username" value="<?php echo $username ?>" />
    <div class="mb-3">
        <label for="password" class="form-label">New password</label>
        <input type="text" class="form-control" id="password" name="password" aria-describedby="password_help">
        <div id="password_help" class="form-text">Make it complex, minimum 8 characters. <strong>Make a note of it. You
                won't see it again!</strong></div>
    </div>
    <button type="submit" class="btn btn-primary">Set password</button>
</form>
</p>

<?php
    require_once('footer.php');
?>