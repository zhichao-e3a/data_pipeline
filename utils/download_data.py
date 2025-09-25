import io
import gzip
import asyncio
import aiohttp
import ssl
import certifi
import random

# from tqdm.auto import tqdm

"""
1XX - Informational (request received and still processing)
2XX - Success
3XX - Redirection
4XX - Client Error (problem with request)
5XX - Server Error (problem with upstream)
"""

RETRYABLE_STATUSES = {
    429,    # Rate limit hit, too many requests
    500,    # Internal server error
    502,    # Bad gateway, proxy or gateway got invalid response from upstream server
    503,    # Service unavailable
    504     # Gateway timeout, no response from upstream server in time
}

RETRYABLE_EXC = (
    asyncio.TimeoutError,
    aiohttp.ServerTimeoutError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    aiohttp.ClientPayloadError,
    aiohttp.TooManyRedirects,
)

def jittered_backoff(attempt, base=2, cap=60):
    return min(cap, base * (2 ** attempt)) * random.uniform(0.5, 1.0)

async def download_gz(
    sem,
    session,
    max_retries,
    url_indexed,
):

    idx, url = url_indexed[0], url_indexed[1]

    if url is None:
        return idx, None

    # Semaphore limit
    async with sem:

        for attempt in range(1, max_retries + 1):

            try:
                async with session.get(url) as response:

                    status = response.status
                    if status in RETRYABLE_STATUSES:
                        retry_after = response.headers.get('Retry-After')
                        delay = float(retry_after) if retry_after else jittered_backoff(attempt)
                        # Return socket to pool
                        await response.release()
                        # Wait a while before continuing with the next attempt
                        await asyncio.sleep(delay)
                        continue

                    response.raise_for_status()
                    content = await response.read()
                    with gzip.open(io.BytesIO(content)) as f:
                        return idx, f.read().decode('utf-8')

            except RETRYABLE_EXC as e:
                msg = f"{idx}: Retryable error exhausted ({type(e).__name__}\n{e}"
                print(msg)
                delay = jittered_backoff(attempt)
                await asyncio.sleep(delay)

            except aiohttp.ClientResponseError as e:
                msg = f"{idx}: HTTP {e.status}\n{e.message}"
                print(msg)

            except Exception as e:
                msg = f"{idx}: Unexpected {type(e).__name__}\n{e}"
                print(msg)

async def process_urls(urls_indexed):

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(
        limit                   = 64,
        use_dns_cache           = True,
        ttl_dns_cache           = 300,
        keepalive_timeout       = 30,
        enable_cleanup_closed   = True,
        ssl                     = ssl_context,
        force_close             = False
    )

    session_timeout = aiohttp.ClientTimeout(total=180)

    semaphore = asyncio.Semaphore(128)

    client_session = aiohttp.ClientSession(
        connector   = connector,
        timeout     = session_timeout,
    )

    tasks, results  = [], []

    async with client_session as session:

        for _url_indexed in urls_indexed:

            # Coroutines are scheduled and start to execute here
            task = asyncio.create_task(
                download_gz(
                    sem         = semaphore,
                    session     = session,
                    max_retries = 3,
                    url_indexed = _url_indexed,
                )
            )

            tasks.append(task)

        # Completed tasks are processed here in terms of completion order
        for fut in asyncio.as_completed(tasks):
            res = await fut
            results.append(res)

        return results

async def async_process_df(df):

    uc, fhr, fmov = df["contraction_url"], df["hb_baby_url"], df["raw_fetal_url"]

    uc_indexed      = [(i,j) for i,j in enumerate(uc)]
    fhr_indexed     = [(i,j) for i,j in enumerate(fhr)]
    fmov_indexed    = [(i,j) for i,j in enumerate(fmov)]

    uc_results, fhr_results, fmov_results = await asyncio.gather(
        process_urls(uc_indexed),
        process_urls(fhr_indexed),
        process_urls(fmov_indexed)
    )

    return uc_results, fhr_results, fmov_results

async def async_process_urls(
        url_list
):

    url_indexed = [(i,j) for i,j in enumerate(url_list)]

    results = await asyncio.gather(
        process_urls(url_indexed),
    )

    return results[0]