
<?php

/**
 * @var Goridge\RelayInterface $relay
 */

use Spiral\RoadRunner\Jobs\Jobs;
use Spiral\Goridge\RPC\RPC;
use Spiral\RoadRunner\Environment;

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$jobs = new Jobs(
	RPC::create(
		Environment::fromGlobals()->getRPCAddress(),
	),
);

$jobs->count();
