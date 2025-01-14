using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosDBImport
{
    public class CosmosScalingManager
    {
        private readonly int _ruCapacity;
        private readonly SemaphoreSlim _ruThrottler = new SemaphoreSlim(1);
        private DateTime _lastBatchTime = DateTime.UtcNow;
        private double _remainingRUs;
        private readonly ConcurrentDictionary<string, DateTime> _partitionKeyLastAccess = new();
        private const int PartitionCooldownMs = 100;

        public CosmosScalingManager(int ruCapacity)
        {
            _ruCapacity = ruCapacity;
            _remainingRUs = ruCapacity;
        }

        public async Task ThrottleBasedOnRU(double requestCharge, string partitionKey)
        {
            await _ruThrottler.WaitAsync();
            try
            {
                var elapsed = (DateTime.UtcNow - _lastBatchTime).TotalSeconds;
                _remainingRUs = Math.Min(_ruCapacity, _remainingRUs + (elapsed * _ruCapacity));

                if (_remainingRUs < requestCharge)
                {
                    var waitTime = TimeSpan.FromSeconds((requestCharge - _remainingRUs) / _ruCapacity);
                    await Task.Delay(waitTime);
                }

                if (_partitionKeyLastAccess.TryGetValue(partitionKey, out DateTime lastAccess))
                {
                    var timeSinceLastAccess = DateTime.UtcNow - lastAccess;
                    if (timeSinceLastAccess.TotalMilliseconds < PartitionCooldownMs)
                    {
                        await Task.Delay(PartitionCooldownMs - (int)timeSinceLastAccess.TotalMilliseconds);
                    }
                }

                _remainingRUs -= requestCharge;
                _lastBatchTime = DateTime.UtcNow;
                _partitionKeyLastAccess.AddOrUpdate(partitionKey, DateTime.UtcNow, (_, __) => DateTime.UtcNow);
            }
            finally
            {
                _ruThrottler.Release();
            }
        }

        public async Task<bool> ShouldRetry(CosmosException ex, int attemptNumber)
        {
            if (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                var delay = ex.RetryAfter ?? TimeSpan.FromSeconds(Math.Pow(2, attemptNumber));
                await Task.Delay(delay);
                return true;
            }
            return false;
        }

        public string CreateDistributedPartitionKey(string baseKey)
        {
            return $"{baseKey}-{Math.Abs(baseKey.GetHashCode() % 10)}";
        }

        public async Task WaitForMemoryPressure()
        {
            long memoryThreshold = 1024L * 1024L * 1024L; // 1GB
            if (GC.GetTotalMemory(false) > memoryThreshold)
            {
                GC.Collect();
                await Task.Delay(1000);
            }
        }
    }
}