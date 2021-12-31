<?php

namespace App;

use Exception;
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
        $this->loadMetadata();
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
     * @throws Exception
     */
    public function getTopic(string $topicName)
    {
        if ( isset($this->topicMap[$topicName]) ) {
            return $this->topicMap[$topicName];
        }
        $topic = new Topic($topicName, $this->config);
        $this->topicMap[$topicName] = $topic;
        return $topic;
    }

    public function close()
    {
        $this->persistMetadata();
        foreach ($this->topicMap as $topic) {
            $topic->close();
        }
    }

    public function loadMetadata()
    {
        $metafile = $this->newMetadataFile();
        if (! file_exists($metafile) ) {
            return;
        }
        $content = file_get_contents($metafile);
        $topics = json_decode($content, true);
        foreach ((array)$topics as $topic) {
            $topic = $this->getTopic($topic['name']);
            foreach ((array)$topics['channels'] as $channelName) {
                $topic->getChannel($channelName);
            }
        }
    }

    public function persistMetadata()
    {
        $metafile = $this->newMetadataFile();
        $topics = [];
        /**@var $topic Topic**/
        foreach ($this->topicMap as $topic) {
            $tmp['name'] = $topic->getName();
            $tmp['channels'] = $topic->getChannels();
            $topics[] = $tmp;
        }
        file_put_contents($metafile, json_encode($topics));
    }

    /**
     * @return string
     */
    public function newMetadataFile(): string
    {
        return sprintf("%s/%s", RUNTIME_PATH, "mq.dat");
    }
}