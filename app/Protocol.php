<?php

namespace App;

use Exception;
use Swoole\Coroutine;

class Protocol
{
    private Client $client;

    private MQ $mq;


    public function __construct(BuffIO $io, MQ $mq)
    {
        $this->client = new Client($io);
        $this->mq = $mq;
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    public function ioLoop()
    {
        $this->messagePump();

        for (;;) {
            try {
                if (! $line = trim($this->client->read("\n")) ) {
                    break;
                }
                $params = explode(' ', $line);
                $method = strtolower($params[0]);
                if (! method_exists($this, $method) ) {
                    $this->client->send("invalid command");
                    break;
                }
                if ($response = call_user_func_array([$this, $method], [$params])) {
                    $this->client->send($response);
                }
            } catch (Exception $exception) {
                $this->client->send($exception->getMessage());
                break;
            }
        }
        $this->client->exitChan->push(true);
        $this->client->subChannelChan->close();
        $this->client->waitGroupWrapper->wait();
    }

    public function messagePump()
    {
        $exit = false;
        $this->client->waitGroupWrapper->add(function () use(&$exit) {
            $exit = $this->client->exitChan->pop();
        });

        $cid = $this->client->waitGroupWrapper->add(function () use(&$exit) {
            while (! $exit ) {
                if (! $subChannel = $this->client->getSubChannel() ) {
                    Coroutine::yield();
                    continue;
                }
                // 可能closed
                if( $message = $subChannel->getMsg() ) {
                    $this->client->send($message->getData());
                }
            }
        });

        $this->client->waitGroupWrapper->add(function () use($cid) {
            if( $subChannel = $this->client->subChannelChan->pop() ) {
                $this->client->setSubChannel($subChannel);
            }
            Coroutine::resume($cid);
        });
    }

    public function nop(array $params)
    {
        return null;
    }

    /**
     * @param array $params
     * @return string
     * @throws Exception
     */
    public function pub(array $params): string
    {
        if ( count($params) < 3 ) {
            throw new Exception("pub command params error");
        }
        $topicName = $params[1];
        $payload = $params[2];
        $topic = $this->mq->getTopic($topicName);
        $message = new Message($this->mq->generateID(), $payload);
        $topic->putMsg($message);
        return 'ok';
    }

    /**
     * @param array $params
     * @return string
     * @throws Exception
     */
    public function sub(array $params): string
    {
        if ( count($params) < 3 ) {
            throw new Exception("sub command params error");
        }
        $topicName = $params[1];
        $channelName = $params[2];
        $topic = $this->mq->getTopic($topicName);
        $channel = $topic->getChannel($channelName);
        $topic->channelUpdateChan->push(true);
        $this->client->subChannelChan->push($channel);
        return 'ok';
    }
}