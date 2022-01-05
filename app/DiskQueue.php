<?php

namespace App;

use Exception;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Timer;

class DiskQueue
{
    protected int $readPos = 0;
    protected int $writePos = 0;
    protected int $readFileNum = 0;
    protected int $writeFileNum = 0;
    protected int $depth = 0;

    protected string $name;
    protected int $maxBytesPerFile;
    protected int $maxBytesPerFileRead;
    protected int $minMsgSize;
    protected int $maxMsgSize;
    protected int $syncEvery;
    protected int $syncTimeout;
    protected int $nextReadPos = 0 ;
    protected int $nextReadFileNum = 0;

    protected $readFile;
    protected $writeFile;

    /**
     * 对外暴露的消息通道
     *
     * @var Channel
     */
    protected Channel $readChan;

    /**
     * 写入的消息数量
     *
     * @var int
     */
    protected int $count = 0;

    /**
     * @var Channel
     */
    protected Channel $exitChan;

    /**
     * @var Channel
     */
    protected Channel $syncChan;

    /**
     * @param string $name
     * @param array $config
     * @throws Exception
     */
    public function __construct(string $name, array $config)
    {
        $this->name = $name;
        $this->maxBytesPerFile = $config['maxBytesPerFile'];
        $this->minMsgSize = $config['minMsgSize'];
        $this->maxMsgSize = $config['maxMsgSize'];
        $this->syncEvery = $config['syncEvery'];
        $this->syncTimeout = $config['syncTimeout'];


        // 对外保留的消息chan
        $this->syncChan = new Channel();
        $this->readChan = new Channel();
        $this->exitChan = new Channel();

        // 收集元数据
        $this->retrieveMetaData();

        // 监听各种chan事件
        $this->ioLoop();
    }


    /**
     * @return string
     */
    protected function metaDataFileName(): string
    {
        return sprintf(('%s/%s.mq.meta.dat'), RUNTIME_PATH, $this->name);
    }

    /**
     * @param int $fileNum
     * @return string
     */
    protected function fileName(int $fileNum): string
    {
        return sprintf(("%s/%s.mq.%06d.dat"), RUNTIME_PATH, $this->name, $fileNum);
    }


    /**
     * @return void
     */
    protected function retrieveMetaData()
    {
        $fileName = $this->metaDataFileName();
        if (! file_exists($fileName) ) {
            Log::info(sprintf("DISK QUEUE(%s) file not exits %s", $this->name, $fileName));
            return ;
        }
        if (! $f = fopen($fileName, 'a+') ) {
            Log::error(sprintf("DISK QUEUE(%s) failed to open %s", $this->name, $fileName));
            return;
        }

        if (! fscanf($f, "%d\t%d\t%d\t%d\t%d",
            $this->depth,
            $this->readFileNum,
            $this->readPos,
            $this->writeFileNum,
            $this->writePos
        )) {
            Log::error(sprintf("DISK QUEUE(%s) failed to scan %s", $this->name, $fileName));
            return;
        }

        $this->nextReadFileNum = $this->readFileNum;
        $this->nextReadPos = $this->readPos;

        $fileName = $this->fileName($this->writeFileNum);
        if (! $f = fopen($fileName, 'r') ) {
            Log::error(sprintf("DISK QUEUE(%s) failed to open %s", $this->name, $fileName));
            return;
        }

        if (! $fileInfo = fstat($f)) {
            Log::error(sprintf("DISK QUEUE(%s) failed to stat %s", $this->name, $fileName));
            return;
        }

        $fileSize = $fileInfo['size'] ?? 0;
        if ($this->writePos < $fileSize) {
            Log::info(sprintf("DISK QUEUE(%s) %s metadata writePos %d < file size of %d, skipping to new file",
                $this->name, $fileName, $this->writePos, $fileSize));
            $this->writeFileNum++;
            $this->writePos = 0;
            if ($this->writeFile != null) {
                fclose($this->writeFile);
                $this->writeFile = null;
            }
        }
    }

    protected function persistMetaData()
    {
        $fileName = $this->metaDataFileName();
        if (! $f = fopen($fileName, 'w+') ) {
            Log::error(sprintf("DISK QUEUE(%s) failed to open %s", $this->name, $fileName));
            return;
        }
        fprintf($f, "%d\t%d\t%d\t%d\t%d",
            $this->depth,
            $this->readFileNum,
            $this->readPos,
            $this->writeFileNum,
            $this->writePos);
        fflush($f);
        fclose($f);

    }

    public function sync()
    {
        if ( $this->writeFile ) {
            fflush($this->writeFile);
        }
        $this->persistMetaData();
    }

    /**
     * @return string
     * @throws Exception
     */
    protected function readOne(): ?string
    {
        if (! $this->readFile ) {
            $curFileName = $this->fileName($this->readFileNum);
            if (! $this->readFile = fopen($curFileName, 'r') ) {
                Log::error(sprintf("DISK QUEUE(%s) failed to open %s", $this->name, $curFileName));
                return null;
            }
            if ( $this->readPos > 0 ) {
                fseek($this->readFile, $this->readPos);
            }
        }
        $this->maxBytesPerFileRead = $this->maxBytesPerFile;
        // 如果写入的文件超过了读取的文件，那么有可能文件是大于最大文件字节的，因为是先写入再比较
        if ( $this->readFileNum < $this->writeFileNum) {
            $fileInfo = fstat($this->readFile);
            $this->maxBytesPerFileRead = $fileInfo['size'];
        }
        if (! $msg = unpack('Nlen', fread($this->readFile, 4)) ) {
            Log::error(sprintf("DISK QUEUE(%s) unpack error", $this->name));
            return null;
        }

        if ( $msg['len'] < $this->minMsgSize || $msg['len'] > $this->maxMsgSize ) {
            Log::error(sprintf("DISK QUEUE(%s) length error", $this->name));
            return null;
        }

        if (! $message = fread($this->readFile, $msg['len']) ) {
            Log::error(sprintf("DISK QUEUE(%s) read error", $this->name));
        }

        $totalBytes = $msg['len'] + 4;
        $this->nextReadPos = $this->readPos + $totalBytes;
        $this->nextReadFileNum = $this->readFileNum;

        if ( $this->readFileNum < $this->writeFileNum && $this->nextReadPos >= $this->maxBytesPerFileRead) {
            if ( $this->readFile != null ) {
                fclose($this->readFile);
                $this->readFile = null;
            }
            $this->nextReadFileNum++;
            $this->nextReadPos = 0;
        }
        return $message;
    }

    /**
     * @param string $data
     * @return bool
     * @throws Exception
     */
    protected function writeOne(string $data): bool
    {
        $dataLen = strlen($data);
        if ( $dataLen > $this->maxMsgSize || $dataLen < $this->minMsgSize ) {
            Log::error(sprintf(
                "invalid message write size (%d) minMsgSize=%d maxMsgSize=%d", $dataLen, $this->minMsgSize, $this->maxMsgSize));
            return false;
        }
        $totalBytes = 4 + $dataLen;
        if ( $this->writePos > 0 && $this->writePos+$totalBytes > $this->maxBytesPerFile) {
            if ( $this->readFileNum == $this->writeFileNum) {
                $this->maxBytesPerFileRead = $this->writePos;
            }
            $this->writeFileNum++;
            $this->writePos = 0;
            $this->sync();
            if ( $this->writeFile != null ) {
                fclose($this->writeFile);
                $this->writeFile = null;
            }
        }
        if (! $this->writeFile ) {
            $curFileName = $this->fileName($this->writeFileNum);
            if (! $this->writeFile = fopen($curFileName, 'a+')) {
                Log::error(sprintf("failed to open %s", $curFileName));
                return false;
            }
            if ( $this->writePos > 0 ) {
                fseek($this->writeFile, $this->writePos);
            }
        }
        if (!  fwrite($this->writeFile, pack('N', $dataLen) . $data) ) {
            Log::error("failed to write");
            return false;
        }
        $this->writePos += $totalBytes;
        $this->depth++;
        return true;
    }

    protected function handleReadError()
    {
        if ( $this->readFileNum == $this->writeFileNum) {
            if ( $this->writeFile != null) {
                fclose($this->writeFile);
                $this->writeFile = null;
            }
            $this->writeFileNum++;
            $this->writePos = 0;
        }
        $badFn = $this->fileName($this->readFileNum);
        $badRenameFn = $badFn . '.bad';
        if (! rename($badFn, $badRenameFn)) {
            Log::info(sprintf("DISK QUEUE(%s) failed to rename bad disk queue file %s to %s", $this->name, $badFn, $badRenameFn));
        }

        $this->readFileNum++;
        $this->readPos = 0;
        $this->nextReadFileNum = $this->readFileNum;
        $this->nextReadPos = 0;
        $this->syncChan->push(true);
        $this->checkTailCorruption($this->depth);
    }

    /**
     * 对外暴露的方法
     *
     * @return Channel
     */
    public function readChan(): Channel
    {
        return $this->readChan;
    }


    public function depth(): int
    {
        return $this->depth;
    }

    /**
     * @param string $data
     * @return bool
     * @throws Exception
     */
    public function put(string $data): bool
    {
        return $this->writeOne($data);
    }

    /**
     * exit
     *
     * @return void
     */
    public function exit()
    {
        $this->exitChan->push(true);
    }

    /**
     * @return void
     * @throws Exception
     */
    protected function moveForward()
    {
        $oldReadFileNum = $this->readFileNum;
        $this->readFileNum = $this->nextReadFileNum;
        $this->readPos = $this->nextReadPos;
        $this->depth--;
        if ($oldReadFileNum != $this->nextReadFileNum) {
            $this->syncChan->push(true);
            $fn = $this->fileName($oldReadFileNum);
            if (! unlink($fn) ) {
                throw new Exception("Unlink file - {$fn}");
            }
        }
        $this->checkTailCorruption($this->depth);
    }

    protected function checkTailCorruption(int $depth)
    {
        if ( $this->readFileNum < $this->writeFileNum || $this->readPos < $this->writePos) {
            return;
        }
        if ($depth != 0) {
            if ($depth < 0) {
                Log::error(sprintf("DISK QUEUE (%s) negative depth at tail (%d), metadata corruption,
                resting 0...", $this->name, $depth));
            } elseif ($depth > 0) {
                Log::error(sprintf("DISK QUEUE (%s) positive depth at tail (%d), data loss,
                resting 0...", $this->name, $depth));
            }
            $this->depth = 0;
            $this->syncChan->push(true);
        }
        if ( $this->readFileNum != $this->writeFileNum || $this->readPos != $this->writePos) {
            if ( $this->readFileNum > $this->writeFileNum) {
                Log::error(sprintf("DISK QUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
                    $this->name, $this->readFileNum, $this->writeFileNum));
            }
            if ( $this->readPos > $this->writePos ) {
                Log::error(sprintf("DISK QUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
                    $this->name, $this->readPos, $this->writePos));
            }
            $this->skipToNextRWFile();
            $this->syncChan->push(true);
        }
    }

    /**
     * @return void
     */
    protected function skipToNextRWFile()
    {
        if ( $this->readFile != null ) {
            fclose($this->readFile);
            $this->readFile = null;
        }

        if ( $this->writeFile != null ) {
            fclose($this->writeFile);
            $this->writeFile = null;
        }

        for ($i = 0; $i < $this->writeFileNum; $i++) {
            $fn = $this->fileName($i);
            if ( file_exists($fn) && !unlink($fn)) {
                Log::error(sprintf("DISK QUEUE(%s) failed to remove data file - %s", $this->name, $fn));
            }
        }
        $this->writeFileNum++;
        $this->writePos = 0;
        $this->readFileNum = $this->writeFileNum;
        $this->readPos = 0;
        $this->nextReadFileNum = $this->writeFileNum;
        $this->nextReadPos = 0;
        $this->depth = 0;
    }

    /**
     * @return void
     */
    protected function countSync()
    {
        $this->count++;
        if ( $this->count == $this->syncEvery) {
            $this->sync();
            $this->count = 0;
        }
    }

    /**
     * ioLoop
     *
     * @return void
     * @throws Exception
     */
    protected function ioLoop()
    {
        $ticker = Timer::tick($this->syncTimeout, function () {
            if ($this->count > 0) {
                $this->sync();
            }
        });

        $exit = false;
        go(function () use(&$exit) {
            while (! $exit) {
                if ( $this->syncChan->pop(1) ) {
                    $this->sync();
                    $this->count = 0;
                }
            }
        });

        go(function () use(&$exit) {
            while(! $exit) {
                if ($this->readFileNum < $this->writeFileNum || $this->readPos < $this->writePos) {
                    if ($this->nextReadPos == $this->readPos) {
                        if ($this->readChan->stats()['consumer_num']) { // 当有通道有消费者时候；
                            if (!$dataRead = $this->readOne()) {
                                $this->handleReadError();
                                continue;
                            }
                            if ($this->readChan->push($dataRead)) {
                                $this->countSync();
                                $this->moveForward();
                            }
                        }
                    }
                }
                Coroutine::sleep(0.01); // 如果没有消息就让出调度
            }
        });

        go(function () use($ticker, &$exit) {
            $exit = $this->exitChan->pop();
            $this->readChan->close();
            Timer::clear($ticker);
        });

    }
}