<?php

date_default_timezone_set('Asia/Shanghai');

require(__DIR__ . "/../../vendor/autoload.php");
require(__DIR__ . "/ExampleConfig.php");

use Aliyun\OTS\OTSClient as OTSClient;

$otsClient = new OTSClient(array(
    'EndPoint' => EXAMPLE_END_POINT,
    'AccessKeyID' => EXAMPLE_ACCESS_KEY_ID,
    'AccessKeySecret' => EXAMPLE_ACCESS_KEY_SECRET,
    'InstanceName' => EXAMPLE_INSTANCE_NAME,
));

foreach (array("UserInfo", "AccountInfo", "BookInfo") as $tableName) {
    $request = array(
        'table_meta' => array(
            'table_name' => $tableName,       // 表名为 MyTable
            'primary_key_schema' => array(
                'PK0' => 'INTEGER',          // 第一个主键列（又叫分片键）名称为PK0, 类型为 INTEGER
                'PK1' => 'STRING',           // 第二个主键列名称为PK1, 类型为STRING
            ),
        ),
        'reserved_throughput' => array(
            'capacity_unit' => array(
                'read' => 0,                 // 预留读写吞吐量设置为：0个读CU，和0个写CU
                'write' => 0,
            ),
        ),
    );
    $otsClient->createTable($request);
}



// 假设有3个表：UserInfo， AccountInfo， 和BookInfo
$response = $otsClient->listTable(array());
print json_encode($response);

/* 样例输出：

[
    "AccountInfo",
    "BookInfo",
    "UserInfo"
]

*/

