<?php
namespace Aliyun\OTS\Handlers;

use com\aliyun\cloudservice\ots2\ColumnType, com\aliyun\cloudservice\ots2\RowExistenceExpectation, com\aliyun\cloudservice\ots2\OperationType, com\aliyun\cloudservice\ots2\Direction, com\aliyun\cloudservice\ots2\Error, com\aliyun\cloudservice\ots2\ColumnSchema, com\aliyun\cloudservice\ots2\ColumnValue, com\aliyun\cloudservice\ots2\Column, com\aliyun\cloudservice\ots2\Row, com\aliyun\cloudservice\ots2\TableMeta, com\aliyun\cloudservice\ots2\Condition, com\aliyun\cloudservice\ots2\CapacityUnit, com\aliyun\cloudservice\ots2\ReservedThroughputDetails, com\aliyun\cloudservice\ots2\ReservedThroughput, com\aliyun\cloudservice\ots2\ConsumedCapacity, com\aliyun\cloudservice\ots2\CreateTableRequest, com\aliyun\cloudservice\ots2\CreateTableResponse, com\aliyun\cloudservice\ots2\UpdateTableRequest, com\aliyun\cloudservice\ots2\UpdateTableResponse, com\aliyun\cloudservice\ots2\DescribeTableRequest, com\aliyun\cloudservice\ots2\DescribeTableResponse, com\aliyun\cloudservice\ots2\ListTableRequest, com\aliyun\cloudservice\ots2\ListTableResponse, com\aliyun\cloudservice\ots2\DeleteTableRequest, com\aliyun\cloudservice\ots2\DeleteTableResponse, com\aliyun\cloudservice\ots2\GetRowRequest, com\aliyun\cloudservice\ots2\GetRowResponse, com\aliyun\cloudservice\ots2\ColumnUpdate, com\aliyun\cloudservice\ots2\UpdateRowRequest, com\aliyun\cloudservice\ots2\UpdateRowResponse, com\aliyun\cloudservice\ots2\PutRowRequest, com\aliyun\cloudservice\ots2\PutRowResponse, com\aliyun\cloudservice\ots2\DeleteRowRequest, com\aliyun\cloudservice\ots2\DeleteRowResponse, com\aliyun\cloudservice\ots2\RowInBatchGetRowRequest, com\aliyun\cloudservice\ots2\TableInBatchGetRowRequest, com\aliyun\cloudservice\ots2\BatchGetRowRequest, com\aliyun\cloudservice\ots2\RowInBatchGetRowResponse, com\aliyun\cloudservice\ots2\TableInBatchGetRowResponse, com\aliyun\cloudservice\ots2\BatchGetRowResponse, com\aliyun\cloudservice\ots2\PutRowInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\UpdateRowInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\DeleteRowInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\TableInBatchWriteRowRequest, com\aliyun\cloudservice\ots2\BatchWriteRowRequest, com\aliyun\cloudservice\ots2\RowInBatchWriteRowResponse, com\aliyun\cloudservice\ots2\TableInBatchWriteRowResponse, com\aliyun\cloudservice\ots2\BatchWriteRowResponse, com\aliyun\cloudservice\ots2\GetRangeRequest, com\aliyun\cloudservice\ots2\GetRangeResponse;

class ProtoBufferEncoder
{
    private function checkParameter($request)
    {
        // TODO implement
    }

    private function preprocessColumnType($type)
    {
        switch ($type) {
            case 'INTEGER': return ColumnType::INTEGER;
            case 'STRING': return ColumnType::STRING;
            case 'BOOLEAN': return ColumnType::BOOLEAN;
            case 'DOUBLE': return ColumnType::DOUBLE;
            case 'BINARY': return ColumnType::BINARY;
            case 'INF_MIN': return ColumnType::INF_MIN;
            case 'INF_MAX': return ColumnType::INF_MAX;
            default:
                throw new \Aliyun\OTS\OTSClientException("Column type must be one of 'INTEGER', 'STRING', 'BOOLEAN', 'DOUBLE', 'BINARY', 'INF_MIN', or 'INF_MAX'.");
        }
    }

    private function preprocessColumnValue($columnValue)
    {
        if (is_bool($columnValue)) {

            // is_bool() is checked before is_int(), to avoid type upcasting
            $columnValue = array('type' => 'BOOLEAN', 'value' => $columnValue);

        } else if (is_int($columnValue)) {
            $columnValue = array('type' => 'INTEGER', 'value' => $columnValue);
        } else if (is_string($columnValue)) {
            $columnValue = array('type' => 'STRING', 'value' => $columnValue);
        } else if (is_double($columnValue) || is_float($columnValue)) {
            $columnValue = array('type' => 'DOUBLE', 'value' => $columnValue);
        } else if (is_array($columnValue)) {
            if (!isset($columnValue['type'])) {
                throw new \Aliyun\OTS\OTSClientException("An array column value must has 'type' field.");
            }

            if ($columnValue['type'] != 'INF_MIN' && $columnValue['type'] != 'INF_MAX' && !isset($columnValue['value'])) {
                throw new \Aliyun\OTS\OTSClientException("A column value wth type INTEGER, STRING, BOOLEAN, DOUBLE, or BINARY must has 'value' field.");
            }
        } else {
            throw new \Aliyun\OTS\OTSClientException("A column value must be a int, string, bool, double, float, or array.");
        }

        $type = $this->preprocessColumnType($columnValue['type']);
        $ret = array('type' => $type);

        switch ($type) {
            case ColumnType::INTEGER: 
                $ret['v_int'] = $columnValue['value'];
                break;
            case ColumnType::STRING: 
                $ret['v_string'] = $columnValue['value'];
                break;
            case ColumnType::BOOLEAN: 
                $ret['v_bool'] = $columnValue['value'];
                break;
            case ColumnType::DOUBLE:
                $ret['v_double'] = $columnValue['value'];
                break;
            case ColumnType::BINARY: 
                $ret['v_binary'] = $columnValue['value'];
                break;
            case ColumnType::INF_MIN:
                break;
            case ColumnType::INF_MAX:
                break;
        }

        return $ret;
    }

    private function preprocessColumns($columns)
    {
        $ret = array();

        foreach ($columns as $name => $value)
        {
            $data = array(
                'name' => $name,
                'value' => $this->preprocessColumnValue($value),
            );
            array_push($ret, $data);
        }

        return $ret;
    }

    private function preprocessCondition($condition)
    {
        switch ($condition) {
            case 'IGNORE':
                return RowExistenceExpectation::IGNORE;
            case 'EXPECT_EXIST':
                return RowExistenceExpectation::EXPECT_EXIST;
            case 'EXPECT_NOT_EXIST':
                return RowExistenceExpectation::EXPECT_NOT_EXIST;
            default:
                throw new \Aliyun\OTS\OTSClientException("Condition must be one of 'IGNORE', 'EXPECT_EXIST' or 'EXPECT_NOT_EXIST'.");
        }
    }

    private function preprocessDeleteRowRequest($request)
    {
        $ret = array();
        $ret['table_name'] = $request['table_name'];
        $ret['condition']['row_existence'] = $this->preprocessCondition($request['condition']);
        $ret['primary_key'] = $this->preprocessColumns($request['primary_key']);
        return $ret;
    }

    private function preprocessCreateTableRequest($request)
    {
        $ret = array();
        $ret['table_meta']['table_name'] = $request['table_meta']['table_name'];
        $ret['reserved_throughput'] = $request['reserved_throughput'];
        foreach ($request['table_meta']['primary_key_schema'] as $k => $v) {
            $name[] = $k;
            $type[] = $this->preprocessColumnType($v);
        }
        for ($i = 0; $i < count($request['table_meta']['primary_key_schema']); $i++) {
            $ret['table_meta']['primary_key_schema'][$i]['name'] = $name[$i];
            $ret['table_meta']['primary_key_schema'][$i]['type'] = $type[$i];
        }
        return $ret;
    }

    private function preprocessPutRowRequest($request)
    {
        // FIXME handle BINARY type
        $ret = array();
        $ret['table_name'] = $request['table_name'];
        $ret['condition'] = array();
        $ret['condition']['row_existence'] = $this->preprocessCondition($request['condition']);
        $ret['primary_key'] = $this->preprocessColumns($request['primary_key']);
     
        if (!isset($request['attribute_columns'])) {
            $request['attribute_columns'] = array();
        }

        $ret['attribute_columns'] = $this->preprocessColumns($request['attribute_columns']);
        return $ret;
    }

    private function preprocessGetRowRequest($request)
    {
        $ret = array();
        $ret['table_name'] = $request['table_name'];
        $ret['primary_key'] = $this->preprocessColumns($request['primary_key']);
        if (!isset($request['columns_to_get'])) {
            $ret['columns_to_get'] = array();
        } else {
            $ret['columns_to_get'] = $request['columns_to_get'];
        }
        return $ret;
    }

    private function preprocessPutInUpdateRowRequest($columnsToPut)
    {
        $ret = array();
        foreach($columnsToPut as $name => $value) {
            $columnData = array(
                'type' => OperationType::PUT,
                'name' => $name,
                'value' => $this->preprocessColumnValue($value),
            );
            array_push($ret, $columnData);
        }
        return $ret;
    }

    private function preprocessDeleteInUpdateRowRequest($columnsToDelete)
    {
        $ret = array();
        foreach ($columnsToDelete as $columnName) {
            array_push($ret, array(
                'type' => OperationType::DELETE,
                'name' => $columnName,
            ));
        }
        return $ret;
    }
    
    private function preprocessUpdateRowRequest($request)
    {
        $ret = array();
        $ret['table_name'] = $request['table_name'];
        $ret['condition']['row_existence'] = $this->preprocessCondition($request['condition']);
        $ret['primary_key'] = $this->preprocessColumns($request['primary_key']);

        $attributeColumns = array();

        if (!empty($request['attribute_columns_to_put'])) {
            $columnsToPut = $this->preprocessPutInUpdateRowRequest($request['attribute_columns_to_put']);
            $attributeColumns = array_merge($attributeColumns, $columnsToPut);
        }

        if (!empty($request['attribute_columns_to_delete'])) {
            $columnsToDelete = $this->preprocessDeleteInUpdateRowRequest($request['attribute_columns_to_delete']);
            $attributeColumns = array_merge($attributeColumns, $columnsToDelete);
        }

        $ret['attribute_columns'] = $attributeColumns;
        return $ret;
    }

    private function preprocessGetRangeRequest($request)
    {
        $ret = array();

        $ret['table_name'] = $request['table_name'];
        switch ($request['direction']) {
            case 'FORWARD':
                $ret['direction'] = Direction::FORWARD;
                break;
            case 'BACKWARD':
                $ret['direction'] = Direction::BACKWARD;
                break;
            default:
                throw new \Aliyun\OTS\OTSClientException("GetRange direction must be 'FORWARD' or 'BACKWARD'.");
        }
     
     
        if (isset($request['columns_to_get'])) {
            $ret['columns_to_get'] = $request['columns_to_get'];
        } else {
            $ret['columns_to_get'] = array();
        }

        if (isset($request['limit'])) {
            $ret['limit'] = $request['limit'];
        }
        $ret['inclusive_start_primary_key'] = $this->preprocessColumns($request['inclusive_start_primary_key']);
        $ret['exclusive_end_primary_key'] = $this->preprocessColumns($request['exclusive_end_primary_key']);
        return $ret;
    }

    private function preprocessBatchGetRowRequest($request)
    {
        $ret = array();
        if (!empty($request['tables'])) {
            for ($i = 0; $i < count($request['tables']); $i++) {
                $ret['tables'][$i]['table_name'] = $request['tables'][$i]['table_name'];
                if (!empty($request['tables'][$i]['columns_to_get'])) {
                    $ret['tables'][$i]['columns_to_get'] = $request['tables'][$i]['columns_to_get'];
                }
                if (!empty($request['tables'][$i]['rows'])) {
                    for ($j = 0; $j < count($request['tables'][$i]['rows']); $j++) {
                        $ret['tables'][$i]['rows'][$j]['primary_key'] = $this->preprocessColumns($request['tables'][$i]['rows'][$j]['primary_key']);
                    }
                }
            }
        }

        return $ret;
    }

    private function preprocessBatchWriteRowRequest($request)
    {
        $ret = array();
        for ($i = 0; $i < count($request['tables']); $i++) {
            $ret['tables'][$i]['table_name'] = $request['tables'][$i]['table_name'];
            if (!empty($request['tables'][$i]['put_rows'])) {
                for ($a = 0; $a < count($request['tables'][$i]['put_rows']); $a++) {
                    $request['tables'][$i]['put_rows'][$a]['table_name'] = "";
                    $ret['tables'][$i]['put_rows'][$a] = $this->preprocessPutRowRequest($request['tables'][$i]['put_rows'][$a]);
                    unset($ret['tables'][$i]['put_rows'][$a]['table_name']);
                }
            }
            if (!empty($request['tables'][$i]['update_rows'])) {
                for ($b = 0; $b < count($request['tables'][$i]['update_rows']); $b++) {
                    $request['tables'][$i]['update_rows'][$b]['table_name'] = "";
                    $ret['tables'][$i]['update_rows'][$b] = $this->preprocessUpdateRowRequest($request['tables'][$i]['update_rows'][$b]);
                    unset($ret['tables'][$i]['update_rows'][$b]['table_name']);
                }
            }
            if (!empty($request['tables'][$i]['delete_rows'])) {
                for ($c = 0; $c < count($request['tables'][$i]['delete_rows']); $c++) {
                    $request['tables'][$i]['delete_rows'][$c]['table_name'] = "";
                    $ret['tables'][$i]['delete_rows'][$c] = $this->preprocessDeleteRowRequest($request['tables'][$i]['delete_rows'][$c]);
                    unset($ret['tables'][$i]['delete_rows'][$c]['table_name']);
                }
            }
        }
        return $ret;
    }

    private function encodeListTableRequest($request)
    {
        return "";
    }
    
    private function encodeDeleteTableRequest($request)
    {
        $pbMessage = new DeleteTableRequest();
        $pbMessage->table_name = $request["table_name"];
                                          
        return $pbMessage->serialize();
    }

    private function encodeDescribeTableRequest($request)
    {
        $pbMessage = new DescribeTableRequest();
        $pbMessage->table_name = $request["table_name"];
                                          
        return $pbMessage->serialize();
    }

    private function encodeUpdateTableRequest($request)
    {
        $pbMessage = new UpdateTableRequest();
        $reservedThroughput = new ReservedThroughput();
        $capacityUnit = new CapacityUnit();
        if(!empty($request['reserved_throughput']['capacity_unit']['read'])){
            $capacityUnit->read = $request['reserved_throughput']['capacity_unit']['read'];
        }
        if(!empty($request['reserved_throughput']['capacity_unit']['write'])){
            $capacityUnit->write = $request['reserved_throughput']['capacity_unit']['write'];
        }
        $reservedThroughput->capacity_unit = $capacityUnit;
                 
        $pbMessage->table_name = $request['table_name'];
        $pbMessage->reserved_throughput = $reservedThroughput;
         
        return $pbMessage->serialize();
    }

    private function encodeCreateTableRequest($request)
    {
        $pbMessage = new CreateTableRequest();
        $tableMeta = new TableMeta();
        $tableName = $tableMeta->table_name = $request['table_meta']['table_name'];
        if (!empty($request['table_meta']['primary_key_schema']))
        {
            for ($i=0; $i < count($request['table_meta']['primary_key_schema']); $i++)
            {
                $columnSchema = new ColumnSchema();
                $columnSchema->name = $request['table_meta']['primary_key_schema'][$i]['name'];
                $columnSchema->type = $request['table_meta']['primary_key_schema'][$i]['type'];
                $tableMeta->primary_key[$i] = $columnSchema;
            }
        }
         
        $reservedThroughput = new ReservedThroughput();
        $capacityUnit = new CapacityUnit();
        $capacityUnit->read = $request['reserved_throughput']['capacity_unit']['read'];
        $capacityUnit->write = $request['reserved_throughput']['capacity_unit']['write'];
        $reservedThroughput->capacity_unit = $capacityUnit;
         
        $pbMessage->table_meta = $tableMeta;
        $pbMessage->reserved_throughput = $reservedThroughput;
         
        return $pbMessage->serialize();
    }

    private function encodeGetRowRequest($request)
    {
        $pbMessage = new GetRowRequest();
        for ($i=0; $i < count($request['primary_key']); $i++)
        {
            $pkColumn = new Column();
            $columnValue = new ColumnValue();
            $pkColumn->name = $request['primary_key'][$i]['name'];
            $columnValue->type = $request['primary_key'][$i]['value']['type'];
            switch ($request['primary_key'][$i]['value']['type'])
            {
                case ColumnType::INTEGER:
                    $columnValue->v_int = $request['primary_key'][$i]['value']['v_int'];
                    break;  
                case ColumnType::STRING:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
                    break;
                case ColumnType::BOOLEAN:
                    $columnValue->v_bool = $request['primary_key'][$i]['value']['v_bool'];
                    break;  
                case ColumnType::DOUBLE:
                    $columnValue->v_double = $request['primary_key'][$i]['value']['v_double'];
                    break;
                case ColumnType::BINARY:
                    $columnValue->v_binary = $request['primary_key'][$i]['value']['v_binary'];
                    break;
                default:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
            }
            $pkColumn->value = $columnValue;
            $pbMessage->primary_key[$i] = $pkColumn;
        }
        if (!empty($request['columns_to_get']))
        {
            for ($i = 0; $i < count($request['columns_to_get']); $i++)
            {
                $pbMessage->columns_to_get[$i] = $request['columns_to_get'][$i];
            }
        }
         
        $pbMessage->table_name = $request['table_name'];
        return $pbMessage->serialize();
    }

    private function encodePutRowRequest($request)
    {
        $pbMessage = new PutRowRequest();
        $condition = new Condition();
        $condition->row_existence = $request['condition']['row_existence'];
         
        for ($i=0; $i < count($request['primary_key']); $i++)
        {
            $pkColumn = new Column();
            $columnValue = new ColumnValue();
            $pkColumn->name = $request['primary_key'][$i]['name'];
            $columnValue->type = $request['primary_key'][$i]['value']['type'];
            switch ($request['primary_key'][$i]['value']['type'])
            {
                case ColumnType::INTEGER:
                    $columnValue->v_int = $request['primary_key'][$i]['value']['v_int'];
                    break;  
                case ColumnType::STRING:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
                    break;
                case ColumnType::BOOLEAN:
                    $columnValue->v_bool = $request['primary_key'][$i]['value']['v_bool'];
                    break;  
                case ColumnType::DOUBLE:
                    $columnValue->v_double = $request['primary_key'][$i]['value']['v_double'];
                    break;
                case ColumnType::BINARY:
                    $columnValue->v_binary = $request['primary_key'][$i]['value']['v_binary'];
                    break;
                default:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
            }
            $pkColumn->value = $columnValue;
            $pbMessage->primary_key[$i] = $pkColumn;
        }
         
        if (!empty($request['attribute_columns']))
        {
            for ($i=0; $i < count($request['attribute_columns']); $i++)
            {
                $attributeColumn = new Column();
                $columnValue = new ColumnValue();
                $attributeColumn->name = $request['attribute_columns'][$i]['name'];
                $columnValue->type = $request['attribute_columns'][$i]['value']['type'];
                switch ($request['attribute_columns'][$i]['value']['type'])
                {
                    case ColumnType::INTEGER:
                        $columnValue->v_int = $request['attribute_columns'][$i]['value']['v_int'];
                        break;  
                    case ColumnType::STRING:
                        $columnValue->v_string = $request['attribute_columns'][$i]['value']['v_string'];
                        break;
                    case ColumnType::BOOLEAN:
                        $columnValue->v_bool = $request['attribute_columns'][$i]['value']['v_bool'];
                        break;  
                    case ColumnType::DOUBLE:
                        $columnValue->v_double = $request['attribute_columns'][$i]['value']['v_double'];
                        break;
                    case ColumnType::BINARY:
                        $columnValue->v_binary = $request['attribute_columns'][$i]['value']['v_binary'];
                        break;
                    default:
                      $columnValue->v_string = $request['attribute_columns'][$i]['value']['v_string'];
                }
                $attributeColumn->value = $columnValue;
                $pbMessage->attribute_columns[$i] = $attributeColumn;
            }
        }
         
        $pbMessage->table_name = $request['table_name'];
        $pbMessage->condition = $condition;
         
        return $pbMessage->serialize();
    }

    private function encodeUpdateRowRequest($request)
    {
        $pbMessage = new UpdateRowRequest();
        $pbMessage->table_name = $request["table_name"];
        $condition = new Condition();
        $condition->row_existence = $request['condition']['row_existence'];
        $pbMessage->condition = $condition;
         
        for ($i=0; $i < count($request['primary_key']); $i++)
        {
            $pkColumn = new Column();
            $columnValue = new ColumnValue();
            $pkColumn->name = $request['primary_key'][$i]['name'];
            $columnValue->type = $request['primary_key'][$i]['value']['type'];
            switch ($request['primary_key'][$i]['value']['type'])
            {
                case ColumnType::INTEGER:
                    $columnValue->v_int = $request['primary_key'][$i]['value']['v_int'];
                    break;  
                case ColumnType::STRING:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
                    break;
                case ColumnType::BOOLEAN:
                    $columnValue->v_bool = $request['primary_key'][$i]['value']['v_bool'];
                    break;  
                case ColumnType::DOUBLE:
                    $columnValue->v_double = $request['primary_key'][$i]['value']['v_double'];
                    break;
                case ColumnType::BINARY:
                    $columnValue->v_binary = $request['primary_key'][$i]['value']['v_binary'];
                    break;
                default:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
            }
            $pkColumn->value = $columnValue;
            $pbMessage->primary_key[$i] = $pkColumn;
        }
         
        if (!empty($request['attribute_columns']))
        {
            for ($i=0; $i < count($request['attribute_columns']); $i++)
            {
                $attributeColumn = new ColumnUpdate();
                $columnValue = new ColumnValue();
                $attributeColumn->name = $request['attribute_columns'][$i]['name'];
                $attributeColumn->type = $request['attribute_columns'][$i]['type'];
                if ($request['attribute_columns'][$i]['type'] == OperationType::DELETE)
                {
                    $pbMessage->attribute_columns[$i] = $attributeColumn;
                    continue;
                }
                 
                $columnValue->type = $request['attribute_columns'][$i]['value']['type'];
                switch ($request['attribute_columns'][$i]['value']['type'])
                {
                    case ColumnType::INTEGER:
                        $columnValue->v_int = $request['attribute_columns'][$i]['value']['v_int'];
                        break;  
                    case ColumnType::STRING:
                        $columnValue->v_string = $request['attribute_columns'][$i]['value']['v_string'];
                        break;
                    case ColumnType::BOOLEAN:
                        $columnValue->v_bool = $request['attribute_columns'][$i]['value']['v_bool'];
                        break;  
                    case ColumnType::DOUBLE:
                        $columnValue->v_double = $request['attribute_columns'][$i]['value']['v_double'];
                        break;
                    case ColumnType::BINARY:
                        $columnValue->v_binary = $request['attribute_columns'][$i]['value']['v_binary'];
                        break;
                    default:
                      $columnValue->v_string = $request['attribute_columns'][$i]['value']['v_string'];
                }
                $attributeColumn->value = $columnValue;
                $pbMessage->attribute_columns[$i] = $attributeColumn;
            }
        }
         
        return $pbMessage->serialize();
    }

    private function encodeDeleteRowRequest($request)
    {
        $pbMessage = new DeleteRowRequest();
        $pbMessage->table_name = $request["table_name"];
        $condition = new Condition();
        $condition->row_existence = $request['condition']['row_existence'];
        $pbMessage->condition = $condition;
         
        for ($i=0; $i < count($request['primary_key']); $i++)
        {
            $pkColumn = new Column();
            $columnValue = new ColumnValue();
            $pkColumn->name = $request['primary_key'][$i]['name'];
            $columnValue->type = $request['primary_key'][$i]['value']['type'];
            switch ($request['primary_key'][$i]['value']['type'])
            {
                case ColumnType::INTEGER:
                    $columnValue->v_int = $request['primary_key'][$i]['value']['v_int'];
                    break;  
                case ColumnType::STRING:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
                    break;
                case ColumnType::BOOLEAN:
                    $columnValue->v_bool = $request['primary_key'][$i]['value']['v_bool'];
                    break;  
                case ColumnType::DOUBLE:
                    $columnValue->v_double = $request['primary_key'][$i]['value']['v_double'];
                    break;
                case ColumnType::BINARY:
                    $columnValue->v_binary = $request['primary_key'][$i]['value']['v_binary'];
                    break;
                default:
                    $columnValue->v_string = $request['primary_key'][$i]['value']['v_string'];
            }
            $pkColumn->value = $columnValue;
            $pbMessage->primary_key[$i] = $pkColumn;
        }
        return $pbMessage->serialize();
    }

    private function encodeBatchGetRowRequest($request)
    {
        $pbMessage = new BatchGetRowRequest();
 
        if(!empty($request['tables'])){
            for ($m = 0; $m < count($request['tables']); $m++) {
                $tableInBatchGetRowRequest = new TableInBatchGetRowRequest();
                $tableInBatchGetRowRequest->table_name = $request['tables'][$m]['table_name'];
                if(!empty($request['tables'][$m]['rows'])){
                    for ($n = 0; $n < count($request['tables'][$m]['rows']); $n++) {
                        $rowInBatchGetRowRequest = new RowInBatchGetRowRequest();
                        for ($i = 0; $i < count($request['tables'][$m]['rows'][$n]['primary_key']); $i++) {
                            $pkColumn = new Column();
                            $columnValue = new ColumnValue();
                            $pkColumn->name = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['name'];
                            $columnValue->type = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['type'];
                            switch ($request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['type']) {
                                case ColumnType::INTEGER:
                                    $columnValue->v_int = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['v_int'];
                                    break;
                                case ColumnType::STRING:
                                    $columnValue->v_string = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['v_string'];
                                    break;
                                case ColumnType::BOOLEAN:
                                    $columnValue->v_bool = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['v_bool'];
                                    break;
                                case ColumnType::DOUBLE:
                                    $columnValue->v_double = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['v_double'];
                                    break;
                                case ColumnType::BINARY:
                                    $columnValue->v_binary = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['v_binary'];
                                    break;
                                default:
                                    $columnValue->v_string = $request['tables'][$m]['rows'][$n]['primary_key'][$i]['value']['v_string'];
                            }
                            $pkColumn->value = $columnValue;
                            $rowInBatchGetRowRequest->primary_key[$i] = $pkColumn;
                        }
                        $tableInBatchGetRowRequest->rows[$n] = $rowInBatchGetRowRequest;
                    }
                }
 
                if (!empty($request['tables'][$m]['columns_to_get'])) {
                    for ($c = 0; $c < count($request['tables'][$m]['columns_to_get']); $c++) {
                        $tableInBatchGetRowRequest->columns_to_get[$c] = $request['tables'][$m]['columns_to_get'][$c];
                    }
                }
                $pbMessage->tables[$m] = $tableInBatchGetRowRequest;
            }
        }
        return $pbMessage->serialize();
    }

    private function encodeBatchWriteRowRequest($request)
    {

        $pbMessage = new BatchWriteRowRequest();

        for ($m = 0; $m < count($request['tables']); $m++) {
            $tableInBatchGetWriteRequest = new TableInBatchWriteRowRequest();
            $tableInBatchGetWriteRequest->table_name = $request['tables'][$m]['table_name'];
            if (!empty($request['tables'][$m]['put_rows'])) {
                for ($p = 0; $p < count($request['tables'][$m]['put_rows']); $p++) {
                    $putRowItem = new PutRowInBatchWriteRowRequest();
                    $condition = new Condition();
                    $condition->row_existence = $request['tables'][$m]['put_rows'][$p]['condition']['row_existence'];
                    $putRowItem->condition = $condition;
 
                    for ($n = 0; $n < count($request['tables'][$m]['put_rows'][$p]['primary_key']); $n++) {
                        $pkColumn = new Column();
                        $columnValue = new ColumnValue();
                        $pkColumn->name = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['name'];
                        $columnValue->type = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['type'];
                        switch ($request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['type']) {
                            case ColumnType::INTEGER:
                                $columnValue->v_int = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['v_int'];
                                break;
                            case ColumnType::STRING:
                                $columnValue->v_string = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['v_string'];
                                break;
                            case ColumnType::BOOLEAN:
                                $columnValue->v_bool = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['v_bool'];
                                break;
                            case ColumnType::DOUBLE:
                                $columnValue->v_double = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['v_double'];
                                break;
                            case ColumnType::BINARY:
                                $columnValue->v_binary = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['v_binary'];
                                break;
                            default:
                                $columnValue->v_string = $request['tables'][$m]['put_rows'][$p]['primary_key'][$n]['value']['v_string'];
                        }
                        $pkColumn->value = $columnValue;
                        $putRowItem->primary_key[$n] = $pkColumn;
                    }
                    if (!empty($request['tables'][$m]['put_rows'][$p]['attribute_columns'])) {
                        for ($c = 0; $c < count($request['tables'][$m]['put_rows'][$p]['attribute_columns']); $c++) {
                            $putRowAttributeColumn = new Column();
                            $putRowColumnValue = new ColumnValue();
                            $putRowAttributeColumn->name = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['name'];
                            $putRowColumnValue->type = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['type'];
                            switch ($request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['type']) {
                                case ColumnType::INTEGER:
                                    $putRowColumnValue->v_int = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['v_int'];
                                    break;
                                case ColumnType::STRING:
                                    $putRowColumnValue->v_string = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['v_string'];
                                    break;
                                case ColumnType::BOOLEAN:
                                    $putRowColumnValue->v_bool = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['v_bool'];
                                    break;
                                case ColumnType::DOUBLE:
                                    $putRowColumnValue->v_double = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['v_double'];
                                    break;
                                case ColumnType::BINARY:
                                    $putRowColumnValue->v_binary = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['v_binary'];
                                    break;
                                default:
                                    $putRowColumnValue->v_string = $request['tables'][$m]['put_rows'][$p]['attribute_columns'][$c]['value']['v_string'];
                            }
                            $putRowAttributeColumn->value = $putRowColumnValue;
                            $putRowItem->attribute_columns[$c] = $putRowAttributeColumn;
                        }
                    }
                    $tableInBatchGetWriteRequest->put_rows[$p] = $putRowItem;
                }
            }
 
            if (!empty($request['tables'][$m]['update_rows'])) {
                for ($j = 0; $j < count($request['tables'][$m]['update_rows']); $j++) {
                    $updateRowItem = new UpdateRowInBatchWriteRowRequest();
                    $condition = new Condition();
                    $condition->row_existence = $request['tables'][$m]['update_rows'][$j]['condition']['row_existence'];
                    $updateRowItem->condition = $condition;
                    for ($b = 0; $b < count($request['tables'][$m]['update_rows'][$j]['primary_key']); $b++) {
                        $pkColumn = new Column();
                        $updateRowColumnValue = new ColumnValue();
                        $pkColumn->name = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['name'];
                        $updateRowColumnValue->type = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['type'];
                        switch ($request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['type']) {
                            case ColumnType::INTEGER:
                                $updateRowColumnValue->v_int = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['v_int'];
                                break;
                            case ColumnType::STRING:
                                $updateRowColumnValue->v_string = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['v_string'];
                                break;
                            case ColumnType::BOOLEAN:
                                $updateRowColumnValue->v_bool = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['v_bool'];
                                break;
                            case ColumnType::DOUBLE:
                                $updateRowColumnValue->v_double = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['v_double'];
                                break;
                            case ColumnType::BINARY:
                                $updateRowColumnValue->v_binary = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['v_binary'];
                                break;
                            default:
                                $updateRowColumnValue->v_string = $request['tables'][$m]['update_rows'][$j]['primary_key'][$b]['value']['v_string'];
                        }
                        $pkColumn->value = $updateRowColumnValue;
                        $updateRowItem->primary_key[$b] = $pkColumn;
                    }
 
                    if (!empty($request['tables'][$m]['update_rows'][$j]['attribute_columns'])) {
                        for ($i = 0; $i < count($request['tables'][$m]['update_rows'][$j]['attribute_columns']); $i++) {
                            $updateRowAttributeColumn = new ColumnUpdate();
                            $updateRowColumnValue = new ColumnValue();
                            $updateRowAttributeColumn->name = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['name'];
                            $updateRowAttributeColumn->type = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['type'];
                            if ($request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['type'] == OperationType::DELETE) {
                                $updateRowItem->attribute_columns[$i] = $updateRowAttributeColumn;
                                continue;
                            }
 
                            $updateRowColumnValue->type = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['type'];
                            switch ($request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['type']) {
                                case ColumnType::INTEGER:
                                    $updateRowColumnValue->v_int = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['v_int'];
                                    break;
                                case ColumnType::STRING:
                                    $updateRowColumnValue->v_string = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['v_string'];
                                    break;
                                case ColumnType::BOOLEAN:
                                    $updateRowColumnValue->v_bool = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['v_bool'];
                                    break;
                                case ColumnType::DOUBLE:
                                    $updateRowColumnValue->v_double = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['v_double'];
                                    break;
                                case ColumnType::BINARY:
                                    $updateRowColumnValue->v_binary = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['v_binary'];
                                    break;
                                default:
                                    $updateRowColumnValue->v_string = $request['tables'][$m]['update_rows'][$j]['attribute_columns'][$i]['value']['v_string'];
                            }
                            $updateRowAttributeColumn->value = $updateRowColumnValue;
                            $updateRowItem->attribute_columns[$i] = $updateRowAttributeColumn;
                        }
                    }
                    $tableInBatchGetWriteRequest->update_rows[$j] = $updateRowItem;
                }
            }
 
            if (!empty($request['tables'][$m]['delete_rows'])) {
                for ($k = 0; $k < count($request['tables'][$m]['delete_rows']); $k++) {
                    $deleteRowItem = new DeleteRowInBatchWriteRowRequest();
                    $condition = new Condition();
                    $condition->row_existence = $request['tables'][$m]['delete_rows'][$k]['condition']['row_existence'];
                    $deleteRowItem->condition = $condition;
                    for ($a = 0; $a < count($request['tables'][$m]['delete_rows'][$k]['primary_key']); $a++) {
                        $pkColumn = new Column();
                        $deleteRowColumnValue = new ColumnValue();
                        $pkColumn->name = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['name'];
                        $deleteRowColumnValue->type = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['type'];
                        switch ($request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['type']) {
                            case ColumnType::INTEGER:
                                $deleteRowColumnValue->v_int = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['v_int'];
                                break;
                            case ColumnType::STRING:
                                $deleteRowColumnValue->v_string = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['v_string'];
                                break;
                            case ColumnType::BOOLEAN:
                                $deleteRowColumnValue->v_bool = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['v_bool'];
                                break;
                            case ColumnType::DOUBLE:
                                $deleteRowColumnValue->v_double = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['v_double'];
                                break;
                            case ColumnType::BINARY:
                                $deleteRowColumnValue->v_binary = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['v_binary'];
                                break;
                            default:
                                $deleteRowColumnValue->v_string = $request['tables'][$m]['delete_rows'][$k]['primary_key'][$a]['value']['v_string'];
                        }
                        $pkColumn->value = $deleteRowColumnValue;
                        $deleteRowItem->primary_key[$a] = $pkColumn;
                    }
                    $tableInBatchGetWriteRequest->delete_rows[$k] = $deleteRowItem;
                }
            }
            //整体设置
            $pbMessage->tables[$m] = $tableInBatchGetWriteRequest;
        }
        return $pbMessage->serialize();

    }

    private function encodeGetRangeRequest($request)
    {

        $pbMessage = new GetRangeRequest();
        for ($i=0; $i < count($request['inclusive_start_primary_key']); $i++)
        {
            $pkColumn = new Column();
            $columnValue = new ColumnValue();
            $pkColumn->name = $request['inclusive_start_primary_key'][$i]['name'];
            $columnValue->type = $request['inclusive_start_primary_key'][$i]['value']['type'];
            switch ($request['inclusive_start_primary_key'][$i]['value']['type'])
            {
                case ColumnType::INTEGER:
                    $columnValue->v_int = $request['inclusive_start_primary_key'][$i]['value']['v_int'];
                    break;  
                case ColumnType::STRING:
                    $columnValue->v_string = $request['inclusive_start_primary_key'][$i]['value']['v_string'];
                    break;
                case ColumnType::BOOLEAN:
                    $columnValue->v_bool = $request['inclusive_start_primary_key'][$i]['value']['v_bool'];
                    break;  
                case ColumnType::DOUBLE:
                    $columnValue->v_double = $request['inclusive_start_primary_key'][$i]['value']['v_double'];
                    break;
                case ColumnType::BINARY:
                    $columnValue->v_binary = $request['inclusive_start_primary_key'][$i]['value']['v_binary'];
                    break;
                default:
                    if(!empty($request['inclusive_start_primary_key'][$i]['value']['v_string'])){
                            $columnValue->v_string = $request['inclusive_start_primary_key'][$i]['value']['v_string'];
                    }
            }
            $pkColumn->value = $columnValue;
            $pbMessage->inclusive_start_primary_key[$i] = $pkColumn;
        }
        for ($i=0; $i < count($request['exclusive_end_primary_key']); $i++)
        {
            $pkColumn = new Column();
            $columnValue = new ColumnValue();
            $pkColumn->name = $request['exclusive_end_primary_key'][$i]['name'];
            $columnValue->type = $request['exclusive_end_primary_key'][$i]['value']['type'];
            switch ($request['exclusive_end_primary_key'][$i]['value']['type'])
            {
                case ColumnType::INTEGER:
                    $columnValue->v_int = $request['exclusive_end_primary_key'][$i]['value']['v_int'];
                    break;  
                case ColumnType::STRING:
                    $columnValue->v_string = $request['exclusive_end_primary_key'][$i]['value']['v_string'];
                    break;
                case ColumnType::BOOLEAN:
                    $columnValue->v_bool = $request['exclusive_end_primary_key'][$i]['value']['v_bool'];
                    break;  
                case ColumnType::DOUBLE:
                    $columnValue->v_double = $request['exclusive_end_primary_key'][$i]['value']['v_double'];
                    break;
                case ColumnType::BINARY:
                    $columnValue->v_binary = $request['exclusive_end_primary_key'][$i]['value']['v_binary'];
                    break;
                default:
                    if(!empty($request['exclusive_end_primary_key'][$i]['value']['v_string'])){
                        $columnValue->v_string = $request['exclusive_end_primary_key'][$i]['value']['v_string'];
                    }
            }
            $pkColumn->value = $columnValue;
            $pbMessage->exclusive_end_primary_key[$i] = $pkColumn;
        }
         
        if (!empty($request['columns_to_get']))
        {
            for ($i = 0; $i < count($request['columns_to_get']); $i++)
            {
                $pbMessage->columns_to_get[$i] = $request['columns_to_get'][$i];
            }
        }
         
        $pbMessage->table_name = $request['table_name'];

        if (isset($request['limit'])) {
            $pbMessage->limit = $request['limit'];
        }
        $pbMessage->direction = $request['direction'];
        return $pbMessage->serialize();

    }

    public function handleBefore($context)
    {
        $request = $context->request;
        $apiName = $context->apiName;

        $debugLogger = $context->clientConfig->debugLogHandler;
        if ($debugLogger != null) {
            $debugLogger("$apiName Request " . json_encode($request));
        }

        $this->checkParameter($apiName, $request);

        // preprocess the request if neccessary 
        $preprocessMethod = "preprocess" . $apiName . "Request";
        if (method_exists($this, $preprocessMethod)) {
            $request = $this->$preprocessMethod($request);
        }

        $encodeMethodName = "encode" . $apiName . "Request";
        $context->requestBody = $this->$encodeMethodName($request);
    }

    public function handleAfter($context)
    {
        if ($context->otsServerException != null) {
            return;
        }
    }
}
