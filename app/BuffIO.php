<?php

namespace App;

use Exception;
use Swoole\Coroutine\Server\Connection;

class BuffIO
{
    private Connection $connection;

    private string $left = '';

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param string $data
     * @return mixed
     */
    public function write(string $data)
    {
        return $this->connection->send($data);
    }

    /**
     * @param string $s
     * @return string
     * @throws Exception
     */
    public function readChar(string $s): string
    {
        for (;;) {
            if (($n = strpos($this->left, $s)) !== false) {
                $msg = substr($this->left, 0, $n + 1);
                $this->left = substr($this->left, $n + 1);
                return $msg;
            }
            if (! $payload = $this->connection->recv() ) {
                $errCode = swoole_last_error();
                $errMsg = socket_strerror($errCode);
                throw new Exception($errMsg, $errCode);
            }
            $this->left .= $payload;
        }
    }

    /**
     * @return bool
     */
    public function close(): bool
    {
        return $this->connection->close();
    }
}