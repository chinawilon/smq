<?php

return [
    'maxMemoryMsgSize' => 1024,
    'maxBytesPerFile' => 1024 << 10,
    'minMsgSize' => 1,
    'maxMsgSize' => 1024,
    'syncEvery' => 10,
    'syncTimeout' => 1000, // 每多少秒同步下disk queue状态
];