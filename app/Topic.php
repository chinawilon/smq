<?php

namespace App;

use Exception;
use Swoole\Coroutine;

class Topic
{
    private string $name;

    private array $channelMap = [];

    private \Swoole\Coroutine\Channel $msgMemoryChan;

    public \Swoole\Coroutine\Channel $channelUpdateChan;

    private int $maxMemoryMsgSize;

    private bool $msgMemoryChanBlock = false;
    private bool $backendChanBlock = false;

    private Coroutine\Channel $exitChan;

    private WaitGroupWrap $waitGroupWrapper;

    private DiskQueue $backend;
    private Coroutine\Channel $backendChan;


    /**
     * @param string $name
     * @param array $config
     * @throws Exception
     */
    public function __construct(string $name, array $config)
    {
        $this->name = $name;
        $this->maxMemoryMsgSize = $config['maxMemoryMsgSize'];
        $this->msgMemoryChan = new \Swoole\Coroutine\Channel($config['maxMemoryMsgSize']);
        $this->backend = new DiskQueue($name, $config);
        $this->backendChan = $this->backend->readChan();
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

        $cid1 = $this->waitGroupWrapper->add(function ()  use(&$exit) {
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

        $cid2 = $this->waitGroupWrapper->add(function ()  use(&$exit) {
            while (! $exit) {
                if ( empty($this->channelMap) ) {
                    $this->backendChanBlock = true;
                    Coroutine::yield();
                    $this->backendChanBlock = false;
                    continue;
                }
                if ($msg = $this->backendChan->pop(1)) {
                    /**@var $channel Channel**/
                    [$id, $payload] = explode(':', $msg);
                    $message = new Message($id, $payload);
                    foreach ($this->channelMap as $channel) {
                        $channel->putMsg($message);
                    }
                }
            }
        });

        $this->waitGroupWrapper->add(function () use($cid1, $cid2, &$exit) {
            while (! $exit) {
                $this->channelUpdateChan->pop(1);
                if ($this->msgMemoryChanBlock) {
                    Coroutine::resume($cid1);
                }
                if($this->backendChanBlock) {
                    Coroutine::resume($cid2);
                }
            }
        });
    }

    /**
     * @param Message $message
     * @return void
     * @throws Exception
     */
    public function putMsg(Message $message)
    {
        if (! $this->msgMemoryChan->isFull() ) {
            $this->msgMemoryChan->push($message);
        } else {
            $this->backend->put($message->getData());
        }
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

    /**
     * @return array
     */
    public function getChannels(): array
    {
        $tmp = [];
        /**@var $channel Channel**/
        foreach($this->channelMap as $channel) {
            $tmp[] = $channel->getName();
        }
        return $tmp;
    }

    /**
     * @return void
     * @throws Exception
     */
    public function close()
    {
        $this->exitChan->push(true);
        $this->waitGroupWrapper->wait();
        $this->backend->exit();

        while (! $this->msgMemoryChan->isEmpty() ) {
            /**@var $message Message*/
            $message = $this->msgMemoryChan->pop();
            $this->backend->put($message->getData());
            $this->backend->sync();
        }
    }
}