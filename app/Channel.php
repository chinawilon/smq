<?php

namespace App;

class Channel
{
    private \Swoole\Coroutine\Channel $memoryMsgChan;

    private string $name;

    public function __construct(string $name, int $maxMemoryMsgSize)
    {
        $this->name = $name;
        $this->memoryMsgChan = new \Swoole\Coroutine\Channel($maxMemoryMsgSize);
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    public function getMsg()
    {
        return $this->memoryMsgChan->pop(1);
    }

    public function putMsg(Message $msg)
    {
        $this->memoryMsgChan->push($msg);
    }
}