using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkloadExecutorMongoDB
{
    public static class ParallelExtensions
    {
        public static async Task ForEachSemaphoreAsync<T>(
            this IEnumerable<T> source,
            int degreeOfParallelism,
            Func<T, Task> body)
        {
            var tasks = new List<Task>();
            using (var throttler = new SemaphoreSlim(degreeOfParallelism))
            {
                foreach (var element in source)
                {
                    await throttler.WaitAsync().ConfigureAwait(false);
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await body(element).ConfigureAwait(false);
                        }
                        finally
                        {
                            throttler.Release();
                        }
                    }));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        public static async Task<TRes[]> ForEachSemaphoreAsync<T, TRes>(
            this IEnumerable<T> source,
            int degreeOfParallelism,
            Func<T, Task<TRes>> body)
        {
            var tasks = new List<Task<TRes>>();
            using (var throttler = new SemaphoreSlim(degreeOfParallelism))
            {
                foreach (var element in source)
                {
                    await throttler.WaitAsync().ConfigureAwait(false);
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            return await body(element).ConfigureAwait(false);
                        }
                        finally
                        {
                            throttler.Release();
                        }
                    }));
                }

                return await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }
    }
}
