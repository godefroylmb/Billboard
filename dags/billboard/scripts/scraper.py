import aiohttp
from bs4 import BeautifulSoup


def quote_if_needed(text):
    if text and "," in text:
        return f'"{text}"'
    return text


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def get_data(date_str, charts):

    url = f"https://www.billboard.com/charts/{charts}/{date_str}"

    async with aiohttp.ClientSession() as session:

        html_content = await fetch(session, url)
        soup = BeautifulSoup(html_content, "html.parser")
        result = soup.select(".o-chart-results-list-row-container")

        content = []

        for res in result:

            title_block = res.select_one("h3")
            song_name = quote_if_needed(title_block.text.strip())
            artist = quote_if_needed(title_block.find_next("span").text.strip())

            rank = int(res.select_one("li span.c-label").text.strip())

            stats = {"LW": "-", "PEAK": "-", "WEEKS": "-"}

            for row in res.select("div.lrv-u-flex"):
                label = row.select_one("span.c-span")
                value = row.select_one("span.c-label")

                if not label or not value:
                    continue

                key = label.text.strip()
                if key in stats:
                    stats[key] = value.text.strip()

            last_week = stats["LW"]
            peak_pos = stats["PEAK"]
            weeks = stats["WEEKS"]

            img_tag = res.select_one(".c-lazy-image__img")
            img_url = None

            if img_tag:
                img_url = img_tag.get("data-lazy-src") or img_tag.get("src")

                if img_url and "lazyload-fallback" in img_url:
                    img_url = None

            content.append([
                date_str,
                song_name,
                artist,
                rank,
                last_week,
                peak_pos,
                weeks,
                img_url
            ])

        return content