<?php
require(__DIR__ . "/../../vendor/autoload.php");

$pblib_dir = __DIR__.'/../../vendor/centraldesktop/protobuf-php/';

exec('protoc -I='.__DIR__.' --proto_path='.__DIR__.'/../../tools/ --plugin=protoc-gen-php="'.$pblib_dir.'/protoc-gen-php.php" \\'.
	'--php_out='.__DIR__.'/../../src/OTS/ProtoBuffer/ '.__DIR__.'/../../tools/ots.proto');