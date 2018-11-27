<?php
/**
 * Implementation of Redis distributed locks
 *
 * @see http://redis.io/topics/distlock
 */

namespace RedLock;

/**
 * Class RedLock
 *
 * @package RedLock
 */
class RedLock
{

    protected $clockDriftFactor = 0.01;

    /**
     * @var \Redis[]
     */
    protected $instances = [];

    protected $quorum;

    protected $retryCount;

    protected $retryDelay;

    /**
     * @var array[]|\Redis[]
     */
    protected $servers = [];

    /**
     * @param array[]|\Redis[] $servers    Array of server tuples [host, port, timeout],
     *                                     or pre-connected \Redis objects
     * @param int              $retryDelay Delay in milliseconds between retries
     * @param int              $retryCount Number of retries to attempt
     */
    public function __construct(array $servers, int $retryDelay = 200, int $retryCount = 3)
    {
        $this->servers = $servers;

        $this->retryDelay = $retryDelay;
        $this->retryCount = $retryCount;

        $this->quorum = min(count($servers), (count($servers) / 2 + 1));
    }

    /**
     * Create the Redis connections
     */
    protected function initInstances()
    {
        if (empty($this->instances)) {
            foreach ($this->servers as $server) {
                if ($server instanceof \Redis) {
                    if ($server->isConnected()) {
                        $redis = $server;
                    } else {
                        throw new \InvalidArgumentException("If you use \\Redis objects as argument, the object must be connected.");
                    }
                } else {
                    if (empty($server[0])) {
                        throw new \InvalidArgumentException("A server hostname or IP is required");
                    }

                    if (empty($server[1])) {
                        $server[1] = 6379;
                    }
                    if (empty($server[2])) {
                        $server[2] = 0;
                    }
                    list($host, $port, $timeout) = $server;
                    $redis = new \Redis();
                    $redis->connect($host, $port, $timeout);
                }
                $this->instances[] = $redis;
            }
        }
    }

    /**
     * @param string $resource Name of the resource to be locked
     * @param int    $ttl      Time in milliseconds for the lock to be held
     * @return array|bool
     */
    public function lock(string $resource, int $ttl = 10000)
    {
        $this->initInstances();

        $token = base64_encode(openssl_random_pseudo_bytes(32));
        $retry = $this->retryCount;

        do {
            $n = 0;

            $startTime = microtime(true) * 1000;

            foreach ($this->instances as $instance) {
                if ($this->lockInstance($instance, $resource, $token, $ttl)) {
                    $n++;
                }
            }

            // Add 2 milliseconds to the drift to account for Redis expires
            // precision, which is 1 millisecond, plus 2 millisecond min drift
            // for small TTLs.
            $drift = ($ttl * $this->clockDriftFactor) + 2;

            $validityTime = $ttl - (microtime(true) * 1000 - $startTime) - $drift;

            if ($n >= $this->quorum && $validityTime > 0) {
                return [
                    'validity' => $validityTime,
                    'resource' => $resource,
                    'token'    => $token,
                ];
            } else {
                foreach ($this->instances as $instance) {
                    $this->unlockInstance($instance, $resource, $token);
                }
            }

            // Wait a random delay before to retry
            $delay = mt_rand(floor($this->retryDelay / 2), $this->retryDelay);
            usleep($delay * 1000);

            $retry--;
        } while ($retry > 0);

        return false;
    }

    /**
     * @param \Redis $instance Server instance to be locked
     * @param string $resource Resource name to be locked
     * @param string $token    Lock token
     * @param int    $ttl      Time to live in milliseconds
     * @return bool
     */
    protected function lockInstance(\Redis $instance, string $resource, string $token, int $ttl): bool
    {
        return $instance->set($resource, $token, ['NX', 'PX' => $ttl]);
    }

    /**
     * @param array $lock Array returned from lock()
     * @return bool
     */
    public function unlock(array $lock): bool
    {
        $this->initInstances();
        $resource = $lock['resource'];
        $token    = $lock['token'];

        $success = 0;
        $fail    = 0;
        foreach ($this->instances as $instance) {
            if ($this->unlockInstance($instance, $resource, $token)) {
                $success += 1;
            } else {
                $fail += 1;
            }
        }

        return $fail == 0;
    }

    /**
     * @param \Redis $instance Server instance to be unlocked
     * @param string $resource Resource name to be unlocked
     * @param string $token    Lock token for verification
     * @return int
     */
    protected function unlockInstance(\Redis $instance, string $resource, string $token): int
    {
        $script = '
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        ';

        // If the redis object is using igBinary as serializer
        // we need to call serialize to make sure we the
        // value is serialized the same way in our above Lua
        // as when we called ->set()
        $serializedToken = $instance->_serialize($token);

        return $instance->eval($script, [$resource, $serializedToken], 1);
    }
}
