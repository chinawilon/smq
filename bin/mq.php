<?php


use App\MQ;
use App\TcpServer;
use Swoole\Coroutine\Server;
use Swoole\Coroutine\Server\Connection;
use Swoole\Process;
use function Co\run;

require __DIR__ . '/../bootstrap/app.php';

run(function () {
    $config = include CONF_PATH . '/mq.php';
    $server = new Server('0.0.0.0', 9500, false, true);
    $tcpServer = new TcpServer(new MQ($config));

    // 中断信号
    Process::signal(SIGINT, function () use($tcpServer, $server) {
        $server->shutdown();
        $tcpServer->close();
        echo 'bye bye!';
    });

    $server->handle(function (Connection $connection) use($tcpServer) {
        $tcpServer->handle($connection);
    });
    $server->start();
});