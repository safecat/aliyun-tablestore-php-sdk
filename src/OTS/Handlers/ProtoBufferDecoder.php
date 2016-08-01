<?php
namespace Aliyun\OTS\Handlers;

use com\aliyun\cloudservice\ots2\ColumnType, com\aliyun\cloudservice\ots2\RowExistenceExpectation, com\aliyun\cloudservice\ots2\OperationType, com\aliyun\cloudservice\ots2\Direction, com\aliyun\cloudservice\ots2\Error, com\aliyun\cloudservice\ots2\ColumnSchema, com\aliyun\cloudservice\ots2\ColumnValue, com\aliyun\cloudservice\ots2\Column, com\aliyun\cloudservice\ots2\Row, com\aliyun\cloudservice\ots2\TableMeta, com\aliyun\cloudservice\ots2\Condition, com\aliyun\cloudservice\ots2\CapacityUnit, com\aliyun\cloudservice\ots2\ReservedThroughputDetails, com\aliyun\cloudservice\ots2\ReservedThroughput, com\aliyun\cloudservice\ots2\ConsumedCapacity, com\aliyun\cloudservice\ots2\CreateTableRequest, com\aliyun\cloudservice\ots2\CreateTableResponse, com\aliyun\cloudservice\ots2\UpdateTableRequest, com\aliyun\cloudservice\ots2\UpdateTableResponse, com\aliyun\cloudservice\ots2\DescribeTableRequest, com\aliyun\cloudservice\ots2\DescribeTableResponse, com\aliyun\cloudservice\ots2\ListTableRequest, com\aliyun\cloudservice\ots2\ListTableResponse, com\aliyun\cloudservice\ots2\DeleteTableRequest, com\aliyun\cloudservice\ots2\DeleteTableResponse, com\aliyun\cloudservice\ots2\GetRowRequest, com\aliyun\cloudservice\ots2\GetRowResponse, com\aliyun\cloudservice\ots2\ColumnUpdate, com\aliyun\cloudservice\ots2\UpdateRowRequest, com\aliyun\cloudservice\ots2\UpdateRowResponse, com\aliyun\cloudservice\ots2\PutRowRequest, com\aliyun\cloudservice\ots2\PutRowResponse, com\aliyun\cloudservice\ots2\DeleteRowRequest, com\aliyun\cloudservice\ots2\DeleteRowResponse, com\aliyun\cloudservice\ots2\RowInBatchGetRowRequest, com\aliyun\cloudservice\ots2\TableInBatchGetRowRequest, com\aliyun\cloudservice\ots2\BatchGetRowRequest, com\aliyun\cloudservice\ots2\RowInBatchGetRowResponse, com\aliyun\cloudservice\ots2\TableInBatchGetRowResponse, com\aliyun\cloudservice\ots2\BatchGetRowResponse, com\aliyun\cloudservice\ots2\PutRowInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\UpdateRowInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\DeleteRowInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\TableInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\BatchWriteRowRequest, com\aliyun\cloudservice\ots2\RowInBatchWriteRowResponse, com\aliyun\cloudservice\ots2\TableInBatchWriteRowResponse, com\aliyun\cloudservice\ots2\BatchWriteRowResponse, com\aliyun\cloudservice\ots2\GetRangeRequest, com\aliyun\cloudservice\ots2\GetRangeResponse;

class ProtoBufferDecoder
{
    public function handleBefore($context)
    {
        // empty
    }

    public function decodeListTableResponse($body) 
    {
        $pbMessage = new ListTableResponse();
        $pbMessage->parse($body);
        $response = array();
        if(!empty($pbMessage->table_names)){
            foreach($pbMessage->table_names as $table_name){
                array_push($response, $table_name);
            }
        }
        return $response;
    }

    public function decodeCreateTableResponse($body) 
    {
        return array();
    }

    public function decodeDeleteTableResponse($body) 
    {
        return array();
    }

    private function parseCapacityUnit($pbMessage)
    {
        return array(
            "capacity_unit" => array(
                "read" => $pbMessage->read,
                "write" => $pbMessage->write
            ),
        );
    }

    private function parseReservedThroughputDetails($pbMessage)
    {
        $capacityUnit = $this->parseCapacityUnit($pbMessage->capacity_unit);

        return array(
            "capacity_unit" => $capacityUnit['capacity_unit'],
            "last_increase_time" => $pbMessage->last_increase_time,
            "last_decrease_time" => $pbMessage->last_decrease_time,
            "number_of_decreases_today" => $pbMessage->number_of_decreases_today
        );
    }

    public function decodeDescribeTableResponse($body) 
    {
        $pbMessage = new DescribeTableResponse();
        $pbMessage->parse($body);
         
        $tableMeta = $pbMessage->table_meta;
         
        $pkSchema = array();
        if(!empty($tableMeta->primary_key)){
            foreach($tableMeta->primary_key as $pkColumn){
                switch ($pkColumn->type)
                {
                    case ColumnType::INTEGER:
                        $columnValueString = "INTEGER";
                        break;  
                    case ColumnType::STRING:
                        $columnValueString = "STRING";
                        break;
                    case ColumnType::BOOLEAN:
                        $columnValueString = "BOOLEAN";
                        break;  
                    case ColumnType::DOUBLE:
                        $columnValueString = "DOUBLE";
                        break;
                    case ColumnType::BINARY:
                        $columnValueString = "BINARY";
                        break;
                    default:
                        throw new OTSClientException("Invalid column type in response.");
                }
                 
                $pkSchema["{$pkColumn->name}"] = $columnValueString;
            }
        }
       
        $response = array(
            "table_meta" => array(
                "table_name" => $tableMeta->table_name,
                "primary_key_schema" => $pkSchema,
            ),
            "capacity_unit_details" => $this->parseReservedThroughputDetails($pbMessage->reserved_throughput_details),
        );
        return $response;
    }

    public function decodeUpdateTableResponse($body) 
    {
        $pbMessage = new UpdateTableResponse();
        $pbMessage->parse($body);
        $response = array(
            "capacity_unit_details" => $this->parseReservedThroughputDetails($pbMessage->reserved_throughput_details),
        );
        return $response;
    }

    private function parseColumns($pbMessage, $type)
    {
        $ret = array();

        if(!empty($pbMessage->$type)){
            foreach($pbMessage->$type as $pkColumn){
                $pkColumnValue= $pkColumn->value;
                switch ($pkColumnValue->type)
                {
                    case ColumnType::INTEGER:
                        $realValue = $pkColumnValue->v_int;
                        break;  
                    case ColumnType::STRING:
                        $realValue = $pkColumnValue->v_string;
                        break;
                    case ColumnType::BOOLEAN:
                        // PB library converts a bool into int, 
                        // we need to convert it back to bool

                        $realValue = $pkColumnValue->v_bool;
                        if ($realValue) {
                            $realValue = true;
                        } else {
                            $realValue = false;
                        }
                        break;  
                    case ColumnType::DOUBLE:
                        $realValue = $pkColumnValue->v_double;
                        break;
                    case ColumnType::BINARY:
                        $realValue = array('type' => 'BINARY', 'value' => $pkColumnValue->v_binary);
                        break;
                    default:
                        throw new OTSClientException("Invalid column type in response.");
                }
                $ret["{$pkColumn->name}"] = $realValue;
            }
        }
        return $ret;
    }

    private function parseRow($pbMessage)
    {
        return array(
            "primary_key_columns" => $this->parseColumns($pbMessage, "primary_key_columns"),
            "attribute_columns" => $this->parseColumns($pbMessage, "attribute_columns"),
        );
    }

    private function parseConsumed($pbMessage)
    {
        return $this->parseCapacityUnit($pbMessage->capacity_unit);
    }

    public function decodeGetRowResponse($body) 
    {
        $pbMessage = new GetRowResponse();
        $pbMessage->parse($body);
        $row = $pbMessage->row;
        $response = array(
            "consumed" => $this->parseConsumed($pbMessage->consumed),
            "row" => $this->parseRow($row),
        );
         
        return $response;
    }

    public function decodePutRowResponse($body) 
    {
        $pbMessage = new PutRowResponse();
        $pbMessage->parse($body);
        $response = array(
            "consumed" => $this->parseConsumed($pbMessage->consumed),
        );
        return $response;
    }

    public function decodeUpdateRowResponse($body)
    {
        $pbMessage = new UpdateRowResponse();
        $pbMessage->parse($body);
        $response = array(
            "consumed" => $this->parseConsumed($pbMessage->consumed),
        );
        return $response;
    }

    public function decodeDeleteRowResponse($body)
    {
        $pbMessage = new DeleteRowResponse();
        $pbMessage->parse($body);
        $response = array(
            "consumed" => $this->parseConsumed($pbMessage->consumed),
        );
        return $response;
    }

    private function parseIsOK($isOK)
    {
        // PB library will treat bool as int
        // we need to convert it back to bool
        if ($isOK) {
            return true;
        } else {
            return false;
        }
    }

    public function decodeBatchGetRowResponse($body) 
    {
        $pbMessage = new BatchGetRowResponse();
        $pbMessage->parse($body);
 
        $tables = array();
        if(!empty($pbMessage->tables)){
            foreach($pbMessage->tables as $tableInBatchGetRow){
                $rowList = array();
                foreach($tableInBatchGetRow as $rowInBatchGetRow){
                    $consumed = $rowInBatchGetRow->consumed;
                    $error = $rowInBatchGetRow->error;
                    $isOK = $this->parseIsOK($rowInBatchGetRow->is_ok);

                    if($isOK)
                    {
                        $rowData = array(
                            "is_ok" => $isOK,
                            "consumed" => $this->parseConsumed($consumed),
                            "row" => $this->parseRow($rowInBatchGetRow->row),
                        );
                    }
                    else
                    {
                        $rowData = array(
                            "is_ok" => $isOK,
                            "error" => array(
                                "code" => $error->code,
                                "message" =>$error->message
                            ),
                        );
                    }
                    array_push($rowList, $rowData);
                }

                array_push($tables, array(
                    "table_name" => $tableInBatchGetRow->table_name,
                    "rows" => $rowList,
                ));
            }
        }

        return array("tables" => $tables);
    }

    private function parseRowsInBatchWriteRow($tableItem, $type)
    {
        $ret = array();

        if(!empty($tableItem->$type)){
            foreach($tableItem->$type as $rowInBatchWriteRow){
                $consumed = $rowInBatchWriteRow->consumed;
                $error = $rowInBatchWriteRow->error;
                $isOK = $rowInBatchWriteRow->is_ok;
                $isOK = $this->parseIsOK($rowInBatchWriteRow->is_ok);

                if ($isOK) {
                    $row = array(
                        "is_ok" => $isOK,
                        "consumed" => $this->parseConsumed($consumed),
                    );
                } else {
                    $row = array(
                        "is_ok" => $isOK,
                        "error" => array(
                            "code" => $error->code,
                            "message" => $error->message
                        ),
                    );
                }
                array_push($ret, $row);
            }
        }
        return $ret;
    }

    public function decodeBatchWriteRowResponse($body) 
    {
        $pbMessage = new BatchWriteRowResponse();
        $pbMessage->parse($body);
        $tables = array();
        if(!empty($pbMessage->tables)){
            foreach($pbMessage->tables as $tableItem){
                $table = array(
                    "table_name" => $tableItem->table_name,
                    "put_rows" => $this->parseRowsInBatchWriteRow($tableItem, 'put_rows'),
                    "update_rows" => $this->parseRowsInBatchWriteRow($tableItem, 'update_rows'),
                    "delete_rows" => $this->parseRowsInBatchWriteRow($tableItem, 'delete_rows'),
                );

                array_push($tables, $table);
            }
        }
        return array("tables" => $tables);
    }

    public function decodeGetRangeResponse($body) 
    {
        $pbMessage = new GetRangeResponse();
        $pbMessage->parse($body);
        $consumed = $pbMessage->consumed;
         
        $rowList = array();
        if(!empty($pbMessage->rows)){
            foreach($pbMessage->rows as $row){
                array_push($rowList, $this->parseRow($row));
            }
        }
        
        $nextStartPrimaryKey = $this->parseColumns($pbMessage, "next_start_primary_key");
        return array(
            "consumed" => $this->parseConsumed($consumed),
            "next_start_primary_key" => $nextStartPrimaryKey,
            "rows" => $rowList,
        );
        return $getrow;
    }

    public function handleAfter($context)
    {
        if ($context->otsServerException != null) {
            return;
        }

        $apiName = $context->apiName;
        $methodName = "decode" . $apiName . "Response";
        $response = $this->$methodName($context->responseBody);
        $context->response = $response;

        $debugLogger = $context->clientConfig->debugLogHandler;
        if ($debugLogger != null) {
            $debugLogger("$apiName Response " . json_encode($response));
        }
    }
}

