<?php
    require_once('header.php');

    // we generate a access key and store it file and send it to
    // the image cache in the form post.
    // the image cache will then call image_cache_login_check.php with the key directly.
    // both use POST so the key transfer is encoded with HTTPS
    
    // they obviously have to logged in to generate a key
    if(!$user){
        echo '<div class="alert alert-danger" role="alert">You do not have permission to access this resource.</div>';
        require_once('footer.php');
        exit;
    }

    $key = get_random_string(64);

    $keys_path = '../data/image_cache_keys/'; // end in slash

    // make the directory if it doesn't exist
    if(!file_exists( $keys_path )) mkdir( $keys_path , 0777, true);

    // remove any keys older than 1 minute (60 seconds) to prevent proliferation
    $files = glob($keys_path . '*.key');
    $now = time();
    foreach ($files as $file) {
        if($now - filemtime($file) > 60) unlink($file);
    }

    // create our file
    file_put_contents( $keys_path . $key . '.key', $now);


?>
    <h2>Log into WFO Image Cache</h2>
    <p>You will be redirected to the WFO Image Cache very shortly. If you are not then something has gone wrong!</p>
    <form id="imageCacheLoginForm" action="<?php echo IMAGE_CACHE_LOGIN_URI ?>" method="POST">
        <input type="hidden" name="key" value="<?php echo $key ?>" />
    </form>
    <script>
        // we immediately submit the form
        document.getElementById("imageCacheLoginForm").submit();
    </script>

    <p><?php echo $key ?></p>

<?php
    require_once('footer.php');

    function get_random_string($length = 16){
        $stringSpace = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $pieces = [];
        $max = mb_strlen($stringSpace, '8bit') - 1;
        for ($i = 0; $i < $length; ++ $i) {
            $pieces[] = $stringSpace[random_int(0, $max)];
        }
        return implode('', $pieces);
    }

?>