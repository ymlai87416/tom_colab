import requests
from bs4 import BeautifulSoup
import datetime
import re
import asyncio
from pyppeteer import launch
import pandas as pd

'''
Scape HKJC horse racing old data.
'''

def process_horse_name(data):
    # exampe is 實力派(E447)
    pattern = r"^(\w+)\((\w+)\)"

    # Use the regular expression to find the matching substring
    match = re.search(pattern, data)

    # Extract the matched group
    if match:
        horse_name = match.group(1)
        horse_id = match.group(2)
        return (horse_name, horse_id)
    
    return None

def process_running_pos(data):
    pattern4 = r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)"
    pattern3 = r"(\d+)\s+(\d+)\s+(\d+)"

    # Use the regular expression to find the matching substring
    match = re.search(pattern4, data)

    # Extract the matched group
    if match:
        run_pos_1 = match.group(1)
        run_pos_2 = match.group(2)
        run_pos_3 = match.group(3)
        run_pos_4 = match.group(4)
        return (run_pos_1, run_pos_2, run_pos_3, run_pos_4)
    
    match = re.search(pattern3, data)

    # Extract the matched group
    if match:
        run_pos_1 = match.group(1)
        run_pos_2 = match.group(2)
        run_pos_3 = match.group(3)
        return (run_pos_1, run_pos_2, run_pos_3, None)

    return (None, None, None, None)

def process_race_str(data):
    result = data.split(" - ")
    return result[0].strip(), result[1].strip(), result[2].strip()

async def download_html(url):
    browser = await launch()
    #browser = await launch(headless=False, args=['--no-sandbox'], options={'javascriptEnabled': True})
    page = await browser.newPage()
    await page.goto(url, {'waitUntil' : ['load', 'domcontentloaded', 'networkidle0', 'networkidle2']})
    html = await page.content()
    await browser.close()
    return html

def get_data(url):
    # Send a GET request to the webpage URL
    #response = requests.get(url)
    html = asyncio.get_event_loop().run_until_complete(download_html(url))

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    if is_empty_page(soup):
        return None

    # load the game data
    race_dict = dict()
    race_tab = soup.find("div", {"class": "race_tab"})
    race_header = ["class", "length", "score", "name", "track_condition", "track_type"]
    rows = race_tab.find("tbody").find_all("tr")
    race_class, race_len, score = process_race_str(rows[1].find_all(["td"])[0].text.strip())
    race_dict[race_header[0]] = race_class
    race_dict[race_header[1]] = race_len
    race_dict[race_header[2]] = score
    race_dict[race_header[3]] = rows[2].find_all(["td"])[0].text.strip()
    race_dict[race_header[4]] = rows[1].find_all(["td"])[2].text.strip()
    race_dict[race_header[5]] = rows[2].find_all(["td"])[2].text.strip()

    # Find the race result table
    result_table = soup.find("table", {"class": "f_tac"})
    header = ["position", "horse_no", "horse_name", "horse_id", "jockey", "trainer", "act. wt.", "declare horse wt.", "LBW", "running pos 1", "running pos 2", "running pos 3", "running pos 4", "win odds", "dr.", "finish time"]

    row_data = []
    # Print the table rows and columns
    for row in result_table.find("tbody").find_all("tr"):
        result = dict()
        cells = row.find_all(["th", "td"])
        result[header[0]] = cells[0].text.strip()
        result[header[1]] = cells[1].text.strip()
        horse_name, horse_id = process_horse_name(cells[2].text.strip())
        result[header[2]] = horse_name
        result[header[3]] = horse_id
        result[header[4]] = cells[3].text.strip()
        result[header[5]] = cells[4].text.strip()
        result[header[6]] = cells[5].text.strip()
        result[header[7]] = cells[6].text.strip()
        result[header[8]] = cells[8].text.strip()
        p1, p2, p3, p4 = process_running_pos(cells[9].text.strip())
        result[header[9]] = p1
        result[header[10]] = p2
        result[header[11]] = p3
        result[header[12]] = p4
        result[header[13]] = cells[11].text.strip()
        result[header[14]] = cells[7].text.strip()
        result[header[15]] = cells[10].text.strip()

        result = race_dict | result
        print(result)

        row_data.append(result)

    return row_data

def is_empty_page(soup):
    data = soup.body.findAll(text=r"沒有相關資料。")

    return len(data) > 0

def download_data(from_date, to_date):
    # URL format https://racing.hkjc.com/racing/information/Chinese/Racing/LocalResults.aspx?RaceDate=2023/03/26&Racecourse=ST&RaceNo=2
    url_format = "https://racing.hkjc.com/racing/information/Chinese/Racing/LocalResults.aspx?RaceDate=%s&Racecourse=%s&RaceNo=%d"
    result = []
    current_date = from_date
    while current_date <= to_date:
        for course in ["ST", "HV"]:
            for race_no in range(1, 11):
                formatted_date = current_date.strftime("%Y/%m/%d")
                url = url_format % (formatted_date, course, race_no)
                #print("debug", url)
                place_data = get_data(url)

                if place_data is None:
                    print("Skipped: %s" % url)
                    continue

                for horse_place in place_data:
                    horse_place["date"] = current_date
                    horse_place["race_no"] = race_no
                    result.append(horse_place)

        current_date += datetime.timedelta(days=1)

    return result

if __name__ == '__main__':
    start_date = datetime.datetime(2023, 3, 26)
    end_date = datetime.datetime(2023, 3, 26)
    result = download_data(start_date, end_date)

    df_data = pd.DataFrame.from_dict(result)
    print(df_data)
    df_data.to_csv("./data/test.csv")