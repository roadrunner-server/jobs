<?php

/**
 * Replies to every job with a payload that is not a protocol message.
 */

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$worker = Spiral\RoadRunner\Worker::create();

while ($worker->waitPayload() !== null) {
	$worker->respond(new Spiral\RoadRunner\Payload('not a protocol message'));
}
