<?php

use Xtendsys\PrestoClass;

require_once './PrestoClass.php';

/**
 * Class PrestoClient
 * presto操作hive操作类.
 */
class PrestoClient
{
    protected $presto;
    protected $data;
    protected $queryColumns = [];

    /**
     * PrestoClient constructor.
     *
     * @param string $ip     IP地址或者url
     * @param string $port   端口号
     * @param string $schema 数据库名称
     */
    public function __construct($ip = '11.11.11.11', $port = '8411', $schema = 'default')
    {
        $this->presto = new PrestoClass("$ip:$port/v1/statement", "hive", $schema);
    }

    /**
     *  根据sql语句返回结果.
     *
     * @param string $sql sql语句
     *
     * @return array|Exception
     *
     * @throws Exception
     */
    public function fetchAll($sql)
    {
        if (empty($sql)) {
            return [];
        }

        try {
            $this->presto->Query($sql);
        } catch (\Exception $e) {
            return $e;
        }
        try {
            $this->presto->WaitQueryExec();

        } catch (\Exception $e) {
            return $this->presto->GetResult(false);
        }
        $this->data = $this->presto->GetData();

        return $this->processData();
    }

    public function fetchRow($sql)
    {
        $result = [];
        $list = $this->fetchAll($sql);
        if (is_array($list) && $list) {
            $result = $list['0'];
        }

        return $result;
    }

    /**
     * @return array
     */
    protected function processData()
    {
        $response = [];
        $this->getQueryColumns();

        foreach ($this->data as $key => $datas) {
            foreach ($datas as $colunmKey => $data) {
                $response[$key][$this->queryColumns[$colunmKey]] = $data;
            }
        }

        return $response;
    }

    /**
     * @return bool
     */
    protected function getQueryColumns()
    {
	$this->queryColumns = [];
        $resultJson = $this->presto->GetResult();
        $result = json_decode($resultJson, true);
        $columns = $result['columns'];
        foreach ($columns as $col) {
            $this->queryColumns[] = $col['name'];
        }

        return true;
    }
}
