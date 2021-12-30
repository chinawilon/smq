<?php

namespace App;

use Swoole\Atomic;

class MQ
{
    private array $topicMap = [];

    private array $config;

    private Atomic $atomic;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->atomic = new Atomic(time());
    }

    /**
     * @return int
     */
    public function generateID(): int
    {
        return $this->atomic->add();
    }

    /**
     * @param string $topicName
     * @return Topic|mixed
     */
    public function getTopic(string $topicName)
    {
        if ( isset($this->topicMap[$topicName]) ) {
            return $this->topicMap[$topicName];
        }
        $topic = new Topic($topicName, $this->config['maxMemoryMsgSize']);
        $this->topicMap[$topicName] = $topic;
        return $topic;
    }

    public function close()
    {
        $metafile = sprintf("%s/mq.topic.dat", RUNTIME_PATH);
        foreach ($this->topicMap as $topic) {
            file_put_contents($metafile, $topic->getName(). "\n", FILE_APPEND);
            $topic->close();
        }
    }
}