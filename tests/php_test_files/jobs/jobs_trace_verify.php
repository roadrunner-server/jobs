<?php

/**
 * @var Goridge\RelayInterface $relay
 */

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

while ($task = $consumer->waitTask()) {
	try {
		$traceparent = $task->getHeaderLine("traceparent");

		if ($traceparent === "") {
			$task->error("traceparent header is missing or empty");
			continue;
		}

		$payload = json_decode($task->getPayload(), true);
		$expected = $payload['traceparent'] ?? '';

		if ($traceparent !== $expected) {
			$task->error("traceparent mismatch: expected=" . $expected . " received=" . $traceparent);
			continue;
		}

		$task->ack();
	} catch (\Throwable $e) {
		$task->error((string)$e);
	}
}
