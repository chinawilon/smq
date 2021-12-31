<?php

namespace App;

class Message
{
    private string $payload;

    private int $id;

    public function __construct(int $id, string $payload)
    {
        $this->id = $id;
        $this->payload = $payload;
    }

    public function getData(): string
    {
        return sprintf("%d:%s", $this->id, $this->payload);
    }

}