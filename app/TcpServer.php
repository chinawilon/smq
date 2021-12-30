<?php

namespace App;

use Exception;
use Swoole\Coroutine\Server\Connection;

class TcpServer
{
    private array $connections = [];

    private MQ $mq;

    public function __construct(MQ $mq)
    {
        $this->mq = $mq;
    }

    public function handle(Connection $connection)
    {
        $buff = new BuffIO($connection);
        try {
            $magicVer = trim($buff->readChar("\n"));
        } catch (Exception $e) {
            $buff->write("need magic version");
            $buff->close();
            return;
        }
        switch ($magicVer) {
            case 'V2':
                $protocol = new Protocol($buff, $this->mq);
                break;
            default:
                $buff->write("invalid magic version");
                $buff->close();
                return;
        }
        $client = $protocol->getClient();
        $peer = implode('#', $connection->exportSocket()->getpeername());
        $this->connections[$peer] = $client;
        $protocol->ioLoop();
        unset($this->connections[$peer]);
    }

    public function close()
    {
        foreach ($this->connections as $client) {
            $client->close();
        }
        $this->mq->close();
    }
}