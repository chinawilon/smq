<?php

namespace App;

use Swoole\Coroutine\WaitGroup;

class WaitGroupWrap
{
    private WaitGroup $waitGroup;

    public function __construct()
    {
        $this->waitGroup = new WaitGroup();
    }

    /**
     * @param int $timeout
     * @return bool
     */
    public function wait(int $timeout = -1): bool
    {
        return $this->waitGroup->wait($timeout);
    }

    /**
     * @param callable $fn
     * @return int
     */
    public function add(callable $fn): int
    {
        $this->waitGroup->add();
        return go(function () use($fn) {
            call_user_func($fn);
            $this->waitGroup->done();
        });
    }
}