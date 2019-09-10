<?php
/**
 * PrestoClient
 * presto操作hive操作类
 *
 * @author kangjun kangjun@qudian.com
 * @date   2016.08.23
 */

use \Xtendsys\PrestoClass;

require_once('./PrestoClass.php');

class PrestoClient
{

    protected $presto;
    protected $data;
    protected $query_columns = [];

    function __construct($ip = '11.11.11.11', $port = '8411')
    {
        $this->presto = new PrestoClass("$ip:$port/v1/statement", "hive");
    }

    public function querySql($sql)
    {
        if (empty($sql)) {
            return [];
        }

        try {
            $this->presto->Query($sql);
        } catch (\Exception $e) {
            return $e;
        }

        $this->presto->WaitQueryExec();
        $this->data = $this->presto->GetData();

        return $this->processData();
    }

    protected function processData()
    {
        $response = [];
        $this->getQueryColumns();

        foreach ($this->data as $key => $datas) {
            foreach ($datas as $colunm_key => $data) {
                $response[$key][$this->query_columns[$colunm_key]] = $data;
            }
        }

        return $response;
    }

    protected function getQueryColumns()
    {
        $result_json = $this->presto->GetResult();
        $result = json_decode($result_json, true);
        $columns = $result['columns'];
        foreach ($columns as $col) {
            $this->query_columns[] = $col['name'];
        }
        return true;
    }
}

$presto = new PrestoClient();
$data = $presto->querySql('show tables'); //from 库名.表名
var_dump($data);
die();

?>
