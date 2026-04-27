<!doctype html>
<?php
    require_once('../config.php');

    // create a user object for use all over
    $user = @$_SESSION['user'] ? $_SESSION['user'] : null;
    
?>

<html lang="en">

<head>
    <!-- Required meta tags -->
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <script src="js/main.js"></script>

    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet"
        integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC" crossorigin="anonymous">

    <link href="style/main.css" rel="stylesheet">

    <title>Fyllo: WFO Content Manager</title>
</head>

<body>


    <nav class="navbar navbar-expand-md navbar-dark fixed-top bg-dark">
        <div class="container-fluid">
            <a class="navbar-brand" href="index.php">Fyllo:</a>
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarCollapse"
                aria-controls="navbarCollapse" aria-expanded="false" aria-label="Toggle navigation">
                <span class="navbar-toggler-icon"></span>
            </button>
            <div class="collapse navbar-collapse" id="navbarCollapse">
                <ul class="navbar-nav me-auto mb-2 mb-md-0">
                    <li class="nav-item">
                        <a class="nav-link <?php echo basename($_SERVER['PHP_SELF']) == 'snippets.php' ? 'active': '';  ?>"
                            aria-current="page" href="snippets.php">Snippets</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link <?php echo basename($_SERVER['PHP_SELF']) == 'facets.php' ? 'active': '';  ?>"
                            aria-current="page" href="facets.php">Facets</a>
                    </li>
                    
                    <li class="nav-item">
                        <a class="nav-link <?php echo basename($_SERVER['PHP_SELF']) == 'sources.php' ? 'active': '';  ?> "
                            href="sources.php">Sources</a>
                    </li>

                    <li class="nav-item">
                        <a class="nav-link <?php echo basename($_SERVER['PHP_SELF']) == 'taxa.php' ? 'active': '';  ?> "
                            href="taxa.php">Taxa</a>
                    </li>


   
                    <?php 
                        if($user){
                            ?>
                    <li class="nav-item">
                        <a class="nav-link <?php echo basename($_SERVER['PHP_SELF']) == 'api.php' ? 'active': '';  ?> "
                            href="api.php">API</a>
                    </li>

                    <li class="nav-item">
                        <a class="nav-link <?php echo $user ? '' : 'disabled'; ?>" href="users.php">Users</a>
                    </li>
                    <?php } ?>
                </ul>
                <form class="d-flex">
                    <?php 
                        if($user){
                            echo '<a class="btn btn-outline-success" href="logout.php" role="button" >Log out: '. $user['username'] .'</a>';
                        }else{
                            echo '<a class="btn btn-outline-success" href="login.php" role="button">Login</a>';
                        }
                    ?>
                </form>
            </div>
        </div>
    </nav>

    <main class="container">
        <div class="bg-light p-5 rounded">