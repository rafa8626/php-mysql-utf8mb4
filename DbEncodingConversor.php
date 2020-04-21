#!/usr/bin/env php
<?php

/**
 * Encoding conversion (MySQL).
 * Fix collation and encoding of a specific database/tables/columns; also, fixes issues with indices.
 * 
 * @see https://mathiasbynens.be/notes/mysql-utf8mb4
 * @see https://medium.com/@alexBerg/my-war-with-mysql-or-how-did-i-switch-to-full-utf8mb4-73b257083ac8
 * @see https://medium.com/tensult/converting-rds-mysql-file-format-from-antelope-to-barracuda-ba8a60b2c1ec
 */
class BdEncodingConversor {
    /**
     * @type int
     */
    private static $_maxLength = 191;

    /**
     * @type PDO
     */
    private $_PDOObject;

    /**
     * @type string
     */
    private $_database;

    /**
     * @type string[]
     */
    private $_indices = [];

    /**
     * @param string $sHost
     * @param string $sDb
     * @return void
     */
    public function __constructor (string $host, string $database) {
        if (!defined('PDO::ATTR_DRIVER_NAME')) {
            echo "PDO is not installed in your system...\n";
            exit;
        }

        $password  = self::_getPassword('Enter MySQL root password: ');
        
        $dns = sprintf('%s:dbname=%s;host=%s;charset=%s', 'mysql', $database, $host, 'utf8mb4');
        try {
            $this->_PDOObject = new PDO($dns, 'root', $password);
            $this->_PDOObject ->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->_PDOObject ->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
            $this->_PDOObject ->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
        } catch (PDOException $e) {
            echo "Connection failed: {$e->getMessage()}\n";
            exit;
        }

        $this->_database = $database;
    }

    public function run() {
        $tablesData = $this->_getDbTablesData($this->_database, $this->_PDOObject);

        $this->_changeDatabaseEncoding();

        $textTypes = ['text','tinytext','mediumtext','longtext', 'varchar','char'];

        // First, convert each text column so, in the event that the type of field
        // needs to be changed to support current byte limits, index can be adjusted as well.
        // To avoid issues during the conversion per columns, turn off foreign keys checking
        $indexStmt = $this->_PDOObject->prepare("SET FOREIGN_KEY_CHECKS=0");
        $indexStmt->execute();

        foreach ($tablesData as $table => $columns) {
            foreach ($columns as $column) {
                if (!in_array($column['type'], $textTypes)) {
                    continue;
                }

                $isNull = $column['nullable'] ? 'NULL' : 'NOT NULL';

                // If type is (VAR)CHAR, adjust the byte limit and mark all indices 
                // associated with it to be updated
                if ($column['type'] === 'char' || $column['type'] === 'varchar') {
                    $this->_verifyColumnStructure($table, $column);
                }

                // Drop old indices
                $this->_dropStoredIndices($table);

                $columnStmt = $this->_PDOObject->prepare("
                      ALTER TABLE  `{$this->_database}`.`{$table}`
                           CHANGE  `{$column['column']}` `{$column['column']}` {$column['type']}
                    CHARACTER SET utf8mb4 
                          COLLATE utf8mb4_unicode_ci
                          {$isNull}");
                $columnStmt->execute();
                echo "Updated encoding for column `{$table}.{$column['column']}` with type {$column['type']}\n";

                // Add updated indices
                $this->_addStoredIndices($table);
            }
        }
        
        foreach (array_keys($aDbInfo) as $table) {
            // This statements allows the table to accept the `Barracuda` file type properly
            $oStmt = $this->_PDOObject->prepare("ALTER TABLE `{$sDb}`.`{$table}` ROW_FORMAT=DYNAMIC");
            $oStmt->execute();
            
            $oStmt = $this->_PDOObject->prepare("
                             ALTER TABLE `{$sDb}`.`{$table}` 
                CONVERT TO CHARACTER SET utf8mb4
                                 COLLATE utf8mb4_unicode_ci");
            $oStmt->execute();
            
            $oRepairStmt = $this->_PDOObject->prepare("REPAIR TABLE `{$sDb}`.`{$table}`");
            $oRepairStmt->execute();
            
            $oOptimizeStmt = $this->_PDOObject->prepare("OPTIMIZE TABLE `{$sDb}`.`{$table}`");
            $oOptimizeStmt->execute();
            
            echo "Fixed encoding/repaired/optimized `{$table}`\n";
        }

        // Turn on foreign keys checking once the operation is completed
        $indexStmt = $this->_PDOObject->prepare("SET FOREIGN_KEY_CHECKS=1");
        $indexStmt->execute();
    }

    /**
     * @param string $sPrompt
     * @return string
     */
    private static function _getPassword (string $sPrompt = "Enter Password: ") {
        echo $sPrompt;
        system('stty -echo');
        $sPassword = trim(fgets(STDIN));
        system('stty echo');
        echo "\n";
        return $sPassword;
    }

    private function _getDbTablesData() {
        $query = "
            SELECT  table_name, column_name, data_type, IF(is_nullable='YES', 1, 0) as nullable, 
                    LENGTH(column_name) as length
              FROM  `information_schema`.`columns` 
             WHERE  table_schema = '{$this->_database}'";

        $tablesData = [];
        foreach ($this->_PDOObject->query($query) as $row) {
            $tablesData[$row['table_name']][] = [
                'column'   => $row['column_name'],
                'type'     => $row['data_type'],
                'nullable' => $row['nullable'],
            ];
        }

        return $tablesData;
    }

    private function _changeDatabaseEncoding() {
        $oStmt = $this->_PDOObject->prepare("ALTER DATABASE `{$this->_database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        $oStmt->execute();
        echo "Altered database charset and collation\n";
    }

    private function _verifyColumnStructure(string $table, array &$columnData) {
        $statement = $this->_PDOObject->prepare("
            SELECT 1 FROM `{$sDb}`.`{$table}` WHERE LENGTH({$columnData['column']}) > {self::$_maxLength}");
        $statement->execute();
        $result = $statement->fetch();

        if ($result) {
            $columnData['type'] = 'TINYTEXT';
            echo "Changed field type for `{$table}.{$column['column']}`\n";
            $this->_collectColumnIndexRef($table, $column['column']);
        } else {
            $columnData['type'] .= "({self::$_maxLength})";
        }
    }

    private function _collectColumnIndexRef(string $table, string $column) {
        $results = $this->_PDOObject->query("SHOW INDEX FROM `{$this->_database}`.`{$table}`");
        foreach ($results as $index) {
            if ($index['Column_name'] === $column['column']) {
                if (!isset($this->_indices[$index['Key_name']])) {
                    $this->_indices[$index['Key_name']] = [
                        'name'    => $index['Key_name'],
                        'unique'  => $index['Non_unique'] === 0,
                        'columns' => ["`{$column['column']}`({self::$_maxLength})"],
                        'deleted' => false,
                    ];
                }
            }
        }

        foreach ($results as $index) {
            if (isset($this->_indices[$index['Key_name']]) && !$this->_indices[$index['Key_name']]['deleted']) {
                if (in_array("`{$index['Column_name']}`({self::$_maxLength})", $this->_indices[$index['Key_name']]['columns'])) {
                    continue;
                }
                $this->_indices[$index['Key_name']]['columns'][] = '`' . $index['Column_name'] . '`';
            }
        }
    }

    private function _dropStoredIndices(string $table) {
        foreach (array_keys($this->_indices) as $index) {
            $indexStmt = $this->_PDOObject->prepare("ALTER TABLE {$table} DROP INDEX {$index}");
            $indexStmt->execute();
            $this->_indices[$index]['deleted'] = true;
        }
    }

    private function _addStoredIndices(string $table) {
        foreach ($this->_indices as $indexName => $indexData) {
            $indexColumns = implode(',', $indexData['columns']);
            $isUnique = $indexData['unique'] ? 'UNIQUE' : "";

            $indexStmt = $this->_PDOObject->prepare("
                ALTER TABLE {$table} 
                        ADD {$isUnique} INDEX `{$indexData['name']}` ({$indexColumns})");
            $indexStmt->execute();
            unset($this->_indices[$indexName]);

            echo "Updated `{$indexData['name']}` index`\n";
        }
    }
}

$oConversor = new BdEncodingConversor($argv[1], $argv[2]);
$oConversor->run();