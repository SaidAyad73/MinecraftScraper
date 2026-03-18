import utils
from bs4 import BeautifulSoup
import asyncio
import pandas as pd
import os
import shutil
import json
import tqdm
import re
import time
import random
from curl_cffi.requests import AsyncSession,Response
from curl_cffi.requests.exceptions import DNSError

# TODO: Add proxy support
# TODO: Add error handling/retries
# TODO: currently proxy and refresh ever doesn't work fix it
# TODO: test dublicate removal logic
# TODO: workers and refresh_cf_every interaction have problem if workers > refresh_cf_every and i think there is an edge case in refresh logic
# TODO: due to the fact that skins order change with time i't would be better to scrape links of all skins first and then download them to avoid missing any skins it will also mitigate the problem of desync between async tasks
# TODO: solve the captha problem with rotating cf_clearance cookies from different ips
# TODO: Check dublicates when runing from a prescraped urls
# TODO: go to therepy
# TODO: Add Failed Tasks tracker to continue from
# TODO: Test save on exception
#TODO: some skins are removed from the site handle it by regestring it in metadata but with None Value

async def get_request(session,retry_times = 3, delay=2,random_max_wait = 1,**params):
    n_times = 0
    response = Response()
    assert retry_times >= 1, "retry_times must be at least 1"
    while n_times < retry_times:
        try:
            # if n_times > 0:
            #     print(f"[!] Retrying to fetch page, attempt {n_times+1}")
            response = await session.get(**params)
            if response.status_code == 200:
                break
            n_times += 1
            await asyncio.sleep(delay + random.uniform(0, random_max_wait))
        except DNSError as e:
            # print(f"[!] DNS Error occurred: {e}")
            n_times += 1
            await asyncio.sleep(delay + random.uniform(0, random_max_wait))
        except Exception as e:
            # print(f"[!] An error occurred: {e}")
            n_times += 1
            await asyncio.sleep(delay + random.uniform(0, random_max_wait))
    return response


class SkindexScraper:
    FAILED = 'Failed'
    SUCCESS = 'Success'
    DUBLICATE = 'Dublicate'
    def __init__(
        self,
        workers=1,
        save_every=1,
        save_on_exception=False,
        meta_data_file="skins.json",
        imgs_dir="./skindex",
        start_fresh=False,
        max_retries=3,
        raise_on_skin_error=True,
        raise_on_page_error=True,
        headless=True,
        request_timeout=20,
        delay = 2,
        random_max_delay = 1,
        use_proxy=True,
        cf_force_refresh=False,
        refresh_cf_every=10,
        ):
        self.target_url = "https://www.minecraftskins.com/"
        self.workers = workers
        self.save_every = save_every
        self.save_on_exception = save_on_exception
        self.meta_data_file = meta_data_file
        self.meta_data = (
            {}
            if start_fresh
            else (
                json.load(open(meta_data_file, "r",encoding="utf-8"))
                if os.path.exists(meta_data_file)
                else {}
            )
        )
        self.use_proxy = use_proxy
        self.imgs_dir = imgs_dir
        if start_fresh and os.path.exists(imgs_dir):
            shutil.rmtree(imgs_dir)
            os.makedirs(imgs_dir, exist_ok=True)
        if os.path.exists(imgs_dir) == False:
            os.makedirs(imgs_dir, exist_ok=True)
        self.start_fresh = start_fresh
        self.raise_on_skin_error = (
            raise_on_skin_error  # raise error if individual skin parsing failed
        )
        self.raise_on_page_error = (
            raise_on_page_error  # raise error if page parsing fails
        )
        self.max_retries = max_retries
        self.headless = headless
        self.complete_scrape_tasks = 0  # Counter for completed scrape tasks
        self.failed_scrape_tasks = []
        self.request_timeout = request_timeout
        self.cf_force_refresh = cf_force_refresh
        self.refresh_cf_every = refresh_cf_every
        self.delay = delay
        self.random_max_delay = random_max_delay
        if refresh_cf_every < workers:
            print("[!] Warning: refresh_cf_every is less than number of workers, you may need to increase workers.")

    async def _get_cf_bundle(self, use_proxy=False):
        """
        Fetches and returns a new Cloudflare clearance bundle.
        Extracted to avoid repetition.
        """
        return await utils.get_cf_clearance_bundle(
            self.target_url,
            wait=self.request_timeout,
            max_retry=self.max_retries,
            headless=self.headless,
            add_random_delay=True,
            test=True,
            use_proxy=use_proxy,
            raise_on_failure=True,
        )

    # async def _save_metadata(self):
    #     """
    #     Saves metadata to file. Extracted to avoid repetition.
    #     """
    #     with open(self.meta_data_file, "w", encoding="utf-8") as f:
    #         json.dump(self.meta_data, f, ensure_ascii=False, indent=4)
    #     print(f"[+] Saved metadata for {len(self.meta_data)} skins.")

    async def _execute_batch_with_retry(self, batch, cookie_val, ua, proxy_server, fallback_to_cf_refresh=True, batch_num=None,is_skin_urls=False):
        """
        Executes a batch of tasks with automatic retry on failure.
        Extracted to avoid repetition.
        
        Args:
            batch: List of page URLs to process
            cookie_val: Cloudflare clearance cookie
            ua: User agent
            proxy_server: Proxy server URL
            fallback_to_cf_refresh: Whether to retry with new CF bundle on failure
            batch_num: Optional batch number for logging
        """
        semaphore = asyncio.Semaphore(self.workers)
        scrape_function = self.collect_skin_urls if is_skin_urls == False else self.scrape_skin_page

        async def sem_task(**args):
            async with semaphore:
                # print('worker acquired semaphore')
                return await scrape_function(**args)

        cookie_val, ua, proxy_server = await self._get_cf_bundle(use_proxy=self.use_proxy) if cookie_val is None or ua is None else (cookie_val, ua, proxy_server)
        try:
            tasks = [
                asyncio.create_task(sem_task(page_url=page_url, cookie_val=cookie_val, ua=ua))
                for page_url in batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=False)
            return results
        except Exception as e:
            if fallback_to_cf_refresh:
                batch_desc = f"batch {batch_num}" if batch_num else "batch"
                # print(f"[!] Error during scraping {batch_desc}: {e}")
                # print("[*] Attempting to refresh cf_clearance cookie and retry...")
                cookie_val, ua, proxy_server = await self._get_cf_bundle(use_proxy=self.use_proxy)
                tasks = [
                    asyncio.create_task(sem_task(page_url=page_url, cookie_val=cookie_val, ua=ua))
                    for page_url in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=False)
                return results
            else:
                raise e

    async def scrape_skin_page(self, page_url = None, cookie_val = None, ua = None,proxy_server = None,get_cf = False):
        if self.is_skin_duplicate(page_url):
            # print(f'[!] Skin from {page_url} is already downloaded, skipping.')
            return SkindexScraper.DUBLICATE
        
        if get_cf:
            if cookie_val and ua:
                print("[*] WARNING user provided cookie_val and ua, but get_cf=True. Using get_cf=True. all provided values will be ignored.")
            cookie_val, ua, proxy_server = await utils.get_cf_clearance_bundle(
                self.target_url,
                wait=15,
                max_retry=self.max_retries,
                headless=self.headless,
                add_random_delay=True,
                test=True,
                use_proxy=self.use_proxy,
            )

        assert cookie_val is not None and ua is not None, "cf_clearance and user_agent must be provided or use get_cf=True to obtain them"

        skin = page_url
        async with utils.AsyncSession() as session:
            session_params = utils.get_session_params(ua, timeout=self.request_timeout, proxy=proxy_server)
            try:
                response = await get_request(
                    session,
                    retry_times=self.max_retries,
                    delay=self.delay,
                    random_max_wait=self.random_max_delay,
                    url=skin,
                    cookies={"cf_clearance": cookie_val},
                    **session_params,
                )
                if response.status_code == 200:
                    metadata = self.parse_skin_metadata(response.text)
                    skin_id = metadata.get("appdata_skin_id", None)
                    assert (
                        skin_id is not None
                    ), f"No skin_id found in metadata for {skin}"  # Sanity check

                    if skin_id is not None:
                        self.meta_data[skin_id] = metadata
                        # Save skin image
                        download_url = metadata.get("download_url", None)

                        assert (
                            download_url is not None
                        ), f"No download URL for skin {skin_id}"
                        download_url = os.path.join(self.target_url, download_url.lstrip("/"))
                        img_response = await get_request(
                            session,
                            retry_times=self.max_retries,
                            delay=self.delay,
                            random_max_wait=self.random_max_delay,
                            url=download_url,
                            cookies={"cf_clearance": cookie_val},
                            **session_params,
                        )
                        if img_response.status_code == 200:
                            img_path = os.path.join(self.imgs_dir, f"{skin_id}.png")
                            with open(img_path, "wb") as f:
                                f.write(img_response.content)
                        # time.sleep(random.uniform(0.1, 1.0))  # Be polite
                        return SkindexScraper.SUCCESS
                else:
                    # print(
                    #     f"[-] Failed to download skin page {skin}: {response.status_code}"
                    # )
                    assert not self.raise_on_skin_error, f"failed to parse skin {skin} page"
                    return SkindexScraper.FAILED
                
            except Exception as e:
                print(f"[-] Error parsing page {page_url}: {e}")
                if self.raise_on_skin_error:
                    self.failed_scrape_tasks.append(page_url)
                    raise e
                return SkindexScraper.FAILED

    async def collect_skin_urls(self, page_url = None, cookie_val = None, ua = None,proxy_server = None,get_cf = False):

        """
        Collects skin URLs from a given page.
        """
        if get_cf:
            if cookie_val and ua:
                print("[*] WARNING user provided cookie_val and ua, but get_cf=True. Using get_cf=True. all provided values will be ignored.")
            cookie_val, ua, proxy_server = await utils.get_cf_clearance_bundle(
                self.target_url,
                wait=15,
                max_retry=self.max_retries,
                headless=self.headless,
                add_random_delay=True,
                test=True,
                use_proxy=self.use_proxy,
            )
        assert cookie_val is not None and ua is not None, "cf_clearance and user_agent must be provided or use get_cf=True to obtain them"

        async with utils.AsyncSession() as session:
            session_params = utils.get_session_params(ua, timeout=self.request_timeout, proxy=proxy_server)
            skins_urls = []
            response = await get_request(
                session,
                retry_times=self.max_retries,
                delay=self.delay,
                random_max_wait=self.random_max_delay,
                url=page_url,
                cookies={"cf_clearance": cookie_val},
                **session_params,
            )

            if response.status_code == 200:
                # print("[+] Successfully reached the site.")
                skins_urls = self.parse_skins_page(response)
                skins_urls = [self.target_url + skin for skin in skins_urls]
            else:
                # print(f"[-] Failed with status: {response.status_code}")
                # print("[!] Cloudflare may have detected the IP or session mismatch.")
                assert not self.raise_on_page_error, f"failed to parse page {page_url}"
                return None
            return skins_urls


    async def scrape(
        self,
        type="top",
        pages=None,
        use_proxy=False,
        cf_bundle=None,
        urls_json=None,
        scrape_skin_urls = True,
        fallback_to_cf_refresh=True,
        save_collected_urls_to=None,
    ):
        """
        type: top, latest
        pages: index of pages to scrape, None for first 50 pages starting from 1
        urls_json: if provided it will load skin page urls from the json file 
        fallback_to_cf_refresh: if True it will try to refresh cf_clearance cookie if scraping fails once
        """
        assert type == 'top' or type == 'latest', "type must be 'top' or 'latest'"
        # assert pages is not None, 'please provide pages to scrape'
        assert not any(page < 1 or not isinstance(page, int) for page in pages), 'page indices must be >= 1 and integers'
        assert (scrape_skin_urls or urls_json) and not (scrape_skin_urls and urls_json), "either scrape_skin_urls must be True or urls_json must be provided, but not both"
        
        self.complete_scrape_tasks = 0
        pages = self.get_pages(type, pages)

        cookie_val, ua, proxy_server = None, None, None
        need_refresh = cf_bundle is None and self.cf_force_refresh == False
        tasks = []

        try:
            # Step 1: Collect skin page URLs
            skin_page_urls = None
            if scrape_skin_urls:
                batches = [
                    pages[i : min(i + self.refresh_cf_every, len(pages))]
                    for i in range(0, len(pages), self.refresh_cf_every)
                ]
                skin_page_urls = []
                loop = tqdm.tqdm(batches, desc="Collecting skin page URLs", unit="batch")
                failed_pages = []
                for i, batch in enumerate(batches):
                    if need_refresh:
                        print(f'creating new cf clearance bundle for batch {i+1}/{len(batches)}')
                        cookie_val, ua, proxy_server = await self._get_cf_bundle(use_proxy=use_proxy)

                    batch_results = await self._execute_batch_with_retry(
                        batch = batch, cookie_val=cookie_val, ua=ua, proxy_server=proxy_server, 
                        fallback_to_cf_refresh=fallback_to_cf_refresh,
                        batch_num=f"{i+1}/{len(batches)}",
                        is_skin_urls=False,
                    )

                    for i,result in enumerate(batch_results):
                        if result is not None: # if sucssfuly collected urls
                            skin_page_urls.extend(result)
                        else:
                            failed_pages.append(batch[i])
                            
                    loop.set_postfix(total_urls=len(skin_page_urls),failed_pages=len(failed_pages))
                    loop.update(1)
                    if save_collected_urls_to is not None:
                        with open(save_collected_urls_to, "w", encoding="utf-8") as f:
                            json.dump({'links': skin_page_urls}, f, ensure_ascii=False, indent=4)
                        print(f'[+] Saved collected skin page URLs to {save_collected_urls_to}')
                    
            elif urls_json is not None:
                skin_page_urls = self._load_urls_from_json(urls_json)
            else:
                raise ValueError("Either scrape_skin_urls must be True or urls_json must be provided.")

            # print(f'[+] Collected {len(skin_page_urls)} skin URLs from {len(pages)} pages.')

            # Step 2: Scrape individual skins in batches
            print(f'[+] Starting to scrape {len(skin_page_urls)} skin pages...')
            skin_page_urls = [url for url in skin_page_urls if not self.is_skin_duplicate(url)]
            print(f'[+] {len(skin_page_urls)} skin pages to scrape after removing already downloaded skins.')
            batches = [
                skin_page_urls[i : min(i + self.refresh_cf_every, len(skin_page_urls))]
                for i in range(0, len(skin_page_urls), self.refresh_cf_every)
            ]
            print(f'[+] Divided skin URLs into {len(batches)} batches for scraping.')

            loop = tqdm.tqdm(batches, desc="Scraping skin pages", unit="skin")
            dublicates = 0
            failed_tasks = []
            for i, batch in enumerate(loop):
                if need_refresh:
                    print(f'creating new cf clearance bundle for batch {i+1}/{len(batches)}')
                    cookie_val, ua, proxy_server = await self._get_cf_bundle(use_proxy=use_proxy)

                results = await self._execute_batch_with_retry(
                    batch = batch, cookie_val=cookie_val, ua=ua, proxy_server=proxy_server, 
                    fallback_to_cf_refresh=fallback_to_cf_refresh,
                    is_skin_urls=True
                )
                
                dublicates += results.count(SkindexScraper.DUBLICATE)
                failed_tasks += [batch[idx] for idx, res in enumerate(results) if res == SkindexScraper.FAILED]
                
                loop.set_postfix(dublicates = dublicates,total_skins_meta = len(self.meta_data))
                loop.update(1)
                self.save_meta_data()
                
            print(f'[+] Completed scraping {len(skin_page_urls)} skin pages.')
            print('completed all scrape tasks')
            # self.log_tasks_status(pages)
            return skin_page_urls
        except KeyboardInterrupt:
            print(f'\n[!] Scraping interrupted by user. Cancelling all tasks...')
            for task in tasks:
                if not task.done():
                    task.cancel()

            if self.save_on_exception:
                self.save_meta_data()
                # self.log_tasks_status(pages)
            raise e
        except Exception as e:
            print(f"\n[FATAL] A task failed: {e}. Cancelling all other tasks...")
            for task in tasks:
                if not task.done():
                    task.cancel()

            if self.save_on_exception:
                self.save_meta_data()
                # self.log_tasks_status(pages)
            raise e
        except:
            print(f"\n[FATAL] An unknown error occurred. Cancelling all other tasks...")
            for task in tasks:
                if not task.done():
                    task.cancel()

            if self.save_on_exception:
                self.save_meta_data()
                # self.log_tasks_status(pages)
            raise e
    async def _collect_skin_urls(self, pages, use_proxy, need_refresh, fallback_to_cf_refresh, read_from=None,save_to = 'skin_page_urls_temp.json'): # deprected
        """
        Collects all skin URLs from pages
        """
        batches = [
            pages[i:min(i + self.refresh_cf_every, len(pages))] 
            for i in range(0, len(pages), self.refresh_cf_every)
        ]
        results = []
        if read_from is not None:
            if os.path.exists(read_from):
                with open(read_from, "r", encoding='utf-8') as f:
                    existing_urls = json.load(f)['links']
                    results.extend(existing_urls)
                print(f'[+] Loaded {len(existing_urls)} existing URLs from {read_from}')
            else:
                open(read_from, "w", encoding='utf-8').write(json.dumps({'links': []}, ensure_ascii=False, indent=4))

        for i, batch in enumerate(batches):
            if need_refresh:
                print(f'creating new cf clearance bundle for pages {batch[0]} to {batch[-1]}')
                cookie_val, ua, proxy_server = await self._get_cf_bundle(use_proxy=use_proxy)

            batch_results = await self._execute_batch_with_retry(
                batch=batch,
                cookie_val=cookie_val,
                ua=ua,
                proxy_server=proxy_server,
                return_skin_urls=True,
                fallback_to_cf_refresh=fallback_to_cf_refresh,
                is_skin_urls=False,
            )

            results.extend(batch_results)

            with open(save_to, "w", encoding="utf-8") as f:
                json.dump({'links': [url for sublist in results if sublist is not None for url in sublist]}, f, ensure_ascii=False, indent=4)

        return [url for sublist in results if sublist is not None for url in sublist]

    def _load_urls_from_json(self, urls_json):
        """
        Loads skin page URLs from JSON file.
        Extracted to avoid repetition.
        """
        with open(urls_json, "r", encoding='utf-8') as f:
            return json.load(f)['links']
    def get_pages(self,type,pages):
        if type == "top":
            pages = [
                (
                    f"https://www.minecraftskins.com/"
                    if p == 1
                    else f"https://www.minecraftskins.com/{p}/"
                )
                for p in pages
            ]
        elif type == "latest":
            pages = [f"https://www.minecraftskins.com/latest/{p}/" for p in pages]
        return pages

    def parse_skins_page(self, response):
        """
        automaticly discard dublicates with id
        """
        soup = BeautifulSoup(response.text, "html.parser")
        skins = soup.find_all("div", class_="skin")
        skins_ids = [skin['data-id'] for skin in skins]
        before_filter = len(skins)
        skins = [skin for skin,skin_id in zip(skins,skins_ids) if skin_id not in self.meta_data]
        # print(f'{before_filter - len(skins)} skins were dublicates and discarded')
        skins = [skin.find('div',class_ = 'skin-img') for skin in skins]
        skins_urls = []
        for skin in skins:
            skin_url = skin.find("a")["href"]
            skin_url = skin_url.lstrip("/")
            skins_urls.append(skin_url)
        return skins_urls

    def parse_skin_metadata(self, html_content: str) -> dict:
        """
        Parse all metadata from HTML response of Minecraft skin pages.
        
        Extracts:
        - Meta tags (description, keywords, author, og:*, twitter:*)
        - Like count (class: js-limit-likes)
        - Dislike count (class: sid-dislikes)
        - All AppData variables
        
        - Additional script variables (skinPath, avatarPath, commentLength, skinSize)
        
        Args:
            html_content: HTML response as string
            
        Returns:
            dict: Dictionary with name:value pairs of all parsed metadata
            
        Examples:
            >>> metadata = parse_skin_metadata(html_string)
            >>> print(metadata['skin_id'])
            '23812496'
            >>> print(metadata['likes'])
            '60'
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        metadata = {}

        # === 1. Parse all meta tags ===
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            # Get property or name attribute
            key = meta.get('property') or meta.get('name')
            value = meta.get('content')

            if key and value:
                # Normalize key (remove special chars, convert to snake_case)
                normalized_key = key.replace(':', '_').replace('-', '_').lower()
                metadata[normalized_key] = value

        # === 2. Parse likes (js-limit-likes) ===
        likes_elem = soup.find(class_='js-limit-likes')
        if likes_elem:
            metadata['likes'] = likes_elem.get_text(strip=True)

        # === 3. Parse dislikes (sid-dislikes) ===
        dislikes_elem = soup.find(class_='sid-dislikes')
        if dislikes_elem:
            dislikes_role = dislikes_elem.get('data-role', '')
            if dislikes_role:
                metadata['dislikes'] = dislikes_role

        # === 4. Parse download_link ===
        download_elem = soup.find(class_='download_link')
        if download_elem:
            # Get the download URL from the anchor tag
            download_link = download_elem.find('a')
            if download_link:
                metadata['download_url'] = download_link.get('href', '')

        # === 5. Parse script variables (AppData, skinPath, avatarPath, etc.) ===
        scripts = soup.find_all('script')
        for script in scripts:
            content = script.string
            if not content:
                continue

            # Parse AppData object
            appdata_match = re.search(r'var\s+AppData\s*=\s*\{([^}]+)\}', content)
            if appdata_match:
                appdata_content = appdata_match.group(1)
                # Parse key-value pairs from AppData, handling quoted strings with commas
                pairs = re.findall(r"(\w+)\s*:\s*'([^']*)'", appdata_content)
                for key, value in pairs:
                    # Clean up unicode escape sequences like \u0020 for spaces
                    value = value.replace('\\u0020', ' ').strip()
                    metadata[f'appdata_{key}'] = value

            # Parse other script variables
            var_patterns = [
                (r"var\s+skinPath\s*=\s*['\"]([^'\"]+)['\"]", 'skin_path'),
                (r"var\s+avatarPath\s*=\s*['\"]([^'\"]+)['\"]", 'avatar_path'),
                (r"var\s+commentLength\s*=\s*['\"]?(\d+)['\"]?", 'comment_length'),
                (r"var\s+skinSize\s*=\s*(\d+)", 'skin_size'),
            ]

            for pattern, key in var_patterns:
                match = re.search(pattern, content)
                if match:
                    metadata[key] = match.group(1)

        return metadata

    def log_tasks_status(self,pages): # deprected results are not gureanted
        print('Warning log_tasks_status is deprected')
        completed = set(pages) -  set(self.failed_scrape_tasks)
        print(f'Total tasks: {len(pages)}. Completed: {len(completed)}. Failed: {len(self.failed_scrape_tasks)}')
        if os.path.exists('tasks_report.txt'):
            os.makedirs('reports',exist_ok=True)
            shutil.move('tasks_report.txt',f'reports/tasks_report_backup{len(os.listdir("reports"))}.txt')
        with open('tasks_report.txt','w') as f:
            f.write(f'Total tasks: {len(pages)}. Completed: {len(completed)}. Failed: {len(self.failed_scrape_tasks)}\n')
            f.write('Failed tasks URLs:\n')
            for url in self.failed_scrape_tasks:
                f.write(f'{url}\n')

            f.write('Completed tasks URLs:\n')
            for url in completed:
                f.write(f'{url}\n')
    def is_skin_duplicate(self, skin_url):
        try:
            metadata = self.meta_data
            skin_id = skin_url.split('/skin/')[1].split('/')[0]
            return skin_id in metadata
        except:
            return False
    def save_meta_data(self):
        if os.path.exists(self.meta_data_file):
            os.makedirs('meta_data_backups',exist_ok=True)
            new_name = f"meta_data_backups/meta_data_backup_{len(os.listdir('meta_data_backups'))}.json"
            shutil.copy(self.meta_data_file, new_name)
            
        with open(self.meta_data_file, "w", encoding="utf-8") as f:
            json.dump(self.meta_data, f, ensure_ascii=False, indent=4)
        print(f"[+] Saved metadata for {len(self.meta_data)} skins.")
async def main():
    # await utils.save_cf_bundle("https://www.minecraftskins.com/",filepath='cf_bundle.json',max_retries=5)
    # cl_bundle = json.load(open("cf_bundle.json", "r"))
    scraper = SkindexScraper(
        workers=10,
        save_every=1,
        save_on_exception=True,
        start_fresh=False,
        meta_data_file="skins.json",
        imgs_dir="./skindex",
        raise_on_page_error=True,
        raise_on_skin_error=False,
        max_retries=5,
        headless=False,
        use_proxy=False,
        request_timeout=15,
        cf_force_refresh=False,
        refresh_cf_every=1000,
        delay=0.5,
        random_max_delay=0.5,
    )

    results = await scraper.scrape(
        type="latest",
        pages=range(1,1000),
        fallback_to_cf_refresh=True,
        scrape_skin_urls=False,
        # save_collected_urls_to='latest_skin_page_urls.json',
        urls_json = 'latest_skin_page_urls.json',
        # cf_bundle=(cl_bundle["cf_clearance"], cl_bundle["user_agent"]),
    )
    
    # print(results)


asyncio.run(main())
