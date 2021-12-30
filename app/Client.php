<?php

namespace App;

use Exception;

class Client
{
    private BuffIO $reader;

    private BuffIO $writer;

    private ?Channel $subChannel;

    public \Swoole\Coroutine\Channel $subChannelChan;

    public \Swoole\Coroutine\Channel $exitChan;

    public waitGroupWrap $waitGroupWrapper;

    public function __construct(BuffIO $io)
    {
        $this->reader = $io;
        $this->writer = clone $io;
        $this->exitChan = new \Swoole\Coroutine\Channel();
        $this->subChannelChan = new \Swoole\Coroutine\Channel();
        $this->subChannel = null;
        $this->waitGroupWrapper = new WaitGroupWrap();
    }

    /**
     * @return Channel|null
     */
    public function getSubChannel(): ?Channel
    {
        return $this->subChannel;
    }

    /**
     * @param Channel $subChannel
     * @return void
     */
    public function setSubChannel(Channel $subChannel)
    {
        $this->subChannel = $subChannel;
    }

    /**
     * @param string $s
     * @return string
     * @throws Exception
     */
    public function read(string $s): string
    {
        return $this->reader->readChar($s);
    }

    /**
     * @param string $s
     * @return mixed
     */
    public function send(string $s)
    {
        return $this->writer->write($s);
    }

    /**
     * @return void
     */
    public function close()
    {
        $this->reader->close();
        $this->writer->close();
    }
}