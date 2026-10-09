<?php

/**
 * Replies to every job with an empty payload.
 */

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$worker = Spiral\RoadRunner\Worker::create();

while ($worker->waitPayload() !== null) {
	$worker->respond(new Spiral\RoadRunner\Payload(''));
}
