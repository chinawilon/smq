<?php

namespace App;

use Swoole\Coroutine;

class Topic
{
    private string $name;

    private array $channelMap = [];

    private \Swoole\Coroutine\Channel $msgMemoryChan;

    public \Swoole\Coroutine\Channel $channelUpdateChan;

    private int $maxMemoryMsgSize;

    private bool $msgMemoryChanBlock = false;

    private Coroutine\Channel $exitChan;

    private WaitGroupWrap $waitGroupWrapper;


    public function __construct(string $name, int $maxMemoryMsgSize)
    {
        $this->name = $name;
        $this->maxMemoryMsgSize = $maxMemoryMsgSize;
        $this->msgMemoryChan = new \Swoole\Coroutine\Channel($maxMemoryMsgSize);
        $this->channelUpdateChan = new Coroutine\Channel();
        $this->exitChan = new Coroutine\Channel();
        $this->waitGroupWrapper = new WaitGroupWrap();
        $this->messagePump();
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return void
     */
    public function messagePump()
    {
        $exit = false;
        $this->waitGroupWrapper->add(function () use(&$exit) {
            $exit = $this->exitChan->pop();
        });

        $cid = $this->waitGroupWrapper->add(function ()  use(&$exit) {
            while (! $exit) {
                if ( empty($this->channelMap) ) {
                    $this->msgMemoryChanBlock = true;
                    Coroutine::yield();
                    $this->msgMemoryChanBlock = false;
                    continue;
                }
                if ($message = $this->msgMemoryChan->pop(1)) {
                    /**@var $channel Channel**/
                    foreach ($this->channelMap as $channel) {
                        $channel->putMsg($message);
                    }
                }
            }
        });

        $this->waitGroupWrapper->add(function () use($cid, &$exit) {
            while (! $exit) {
                $this->channelUpdateChan->pop(1);
                if ($this->msgMemoryChanBlock) {
                    Coroutine::resume($cid);
                }
            }
        });
    }

    /**
     * @param Message $message
     * @return void
     */
    public function putMsg(Message $message)
    {
        $this->msgMemoryChan->push($message);
    }

    /**
     * @param string $channelName
     * @return Channel
     */
    public function getChannel(string $channelName): Channel
    {
        if ( isset($this->channelMap[$channelName]) ) {
            return $this->channelMap[$channelName];
        }
        $channel = new Channel($channelName, $this->maxMemoryMsgSize);
        $this->channelMap[$channelName] = $channel;
        return $channel;
    }

    public function close()
    {
        $this->exitChan->push(true);
        $this->waitGroupWrapper->wait();

        $metafile = sprintf("%s/topic.%s.dat", RUNTIME_PATH, $this->name);
        while (! $this->msgMemoryChan->isEmpty() ) {
            /**@var $message Message*/
            $message = $this->msgMemoryChan->pop();
            file_put_contents($metafile, $message->getData() ."\n", FILE_APPEND);
        }
    }
}