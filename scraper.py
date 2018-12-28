import argparse
import csv
import json
import logging
import os
import pickle
import re
import traceback
from datetime import datetime
from io import BytesIO

import requests
from bs4 import BeautifulSoup

ROOT_URL = 'http://njdg.ecourts.gov.in/njdgv1'
SESSION = requests.Session()


def solve_image(image):
    data = {'image': image}
    request = requests.post('http://dftly.com/precapt', files=data)
    answer = request.content.decode('utf-8')
    return answer


def read_table_data_and_links(url):
    _data = []  # This basically sets a default value for the function to return.
    try:
        page_request = SESSION.get(url, timeout=10)

        if page_request.status_code == 200:  # Status code check. Most minimal error checking on Earth.
            page_source = page_request.text
            # Every once in a while, NJDG hits a "connection error". It's an error on their server, we don't know what
            # causes it, but we do have to work around it. The next line tests for it.
            if 'Connection Error' not in page_source and 'Connection  Error' not in page_source:
                soup = BeautifulSoup(page_source, 'lxml')

                # NJDG uses a table to position a bunch of additional elements, as well as to display the data.
                # This gets us the table with the data in it.
                table = soup.find_all('table')[1]

                # We read column headers and values in separate loops and then combine them. Trust me, it's easier.
                headers = [th.text.lower() for th in
                           table.select('thead tr:nth-of-type(2) td')]  # Reads column titles from table.
                # The next two lines insert fields for a timestamp and the URL that this row points to.
                headers.insert(0, 'url')
                headers.insert(0, 'timestamp')

                rows = table.select('tbody tr')  # Gets the actual rows from the table.
                for index, row in enumerate(rows):
                    #  This lets us skip the header row - it has a colspan set to it, but the rows with data
                    #  in them don't.
                    cols = [td.text for td in [td for td in row.select('td') if 'colspan' not in td.attrs]]
                    if len(row.select('td:nth-of-type(4) > a')) > 0:
                        url = row.select('td:nth-of-type(4) > a')[0].get('href')  # Get URL pointed to by the row.
                    else:
                        url = ''
                    # The next two lines add the URL and timestamp to our data
                    cols.insert(0, url)
                    cols.insert(0, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    _data.append(dict(list(zip(headers, cols))))  # Combine headers and columns to create a dictionary

                _data = [x for x in _data if x]  # Null check.
        # These else and except clauses catch connection and timeout errors, flagging them for future re-scraping.
        else:
            _data = [{'url': url, 'error_flag': True, 'done': False}]
            logger.warning('Encountered error on URL {}'.format(url))
    except requests.Timeout:
        _data = [{'url': url, 'error_flag': True, 'done': False}]
        logger.warning('Encountered error on URL {}'.format(url))
    return _data


def get_to_cases_root():
    home_page_request = SESSION.get(ROOT_URL + '/index.php')  # Sets PHPSESSID cookie

    # NJDG uses CSRF-Magic, so we need this token in order to POST the captcha code successfully. Without it,
    # the POST request will fail.
    csrf_token = re.search(r'var csrfMagicToken = \"(.*?)\"', home_page_request.text).group(1)

    logger.info('Attempting to solve captcha...')
    captcha_image = SESSION.get(ROOT_URL + '/securimage/securimage_show.php').content  # Downloads captcha image
    # Python's typing is weird. Safer to use a BytesIO object than use the text directly.
    captcha_image = BytesIO(captcha_image)
    captcha_code = solve_image(captcha_image)  # Uses our ConvNet to break the captcha
    # Posts the solved captcha to NJDG. This sets up session cookies so that we can make the rest of our requests.
    solve_request = SESSION.post(ROOT_URL + '/o_index.php',
                                 data={'__csrf_magic': csrf_token, 'captcha': captcha_code, 'guestlogin': 'Go'})

    # The next few lines test for a successful solve. The ConvNet is ~90% accurate with captcha solves,
    # so we do occasionally hit bad solves.
    soup = BeautifulSoup(solve_request.text, 'lxml')
    if soup.find('iframe', src='frames.php'):
        # The top-level NJDG page has an iframe with src 'frames.php'. If we're able to find that iframe, it means that
        # the captcha solve was successful.
        logger.info('Successfully solved captcha')
    else:
        # In case of a bad solve
        logger.warning('Incorrect captcha, retrying...')
        get_to_cases_root()  # Recursion. Because while-loops are for losers.


def write_judge_data_to_csv(output_file, data):
    with open(output_file, 'w+') as output_fp:
        fieldnames = list(data[0]['districts'][0]['courts'][0]['judges'][0].keys())
        fieldnames.append('establishment')
        fieldnames.append('district')
        fieldnames.append('state')
        writer = csv.DictWriter(output_fp, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for state in data:
            for district in state.get('districts', []):
                for court in district.get('courts', []):
                    for judge in court.get('judges', []):
                        judge_data = judge
                        judge_data['establishment'] = court['establishment']
                        judge_data['district'] = district['district']
                        judge_data['state'] = state['state']
                        writer.writerow(judge_data)
        os._exit(0)


def scrape_summary(output_file, category='totalpending_cases'):
    base_url = ROOT_URL + '/stat_reports/national_detail.php?objection1={}&type=both'.format(category)
    shutdown_by_keyboard_interrupt = False
    counter = 0

    if not os.path.exists('./cache.json'):  # Crash resumption FTW!
        # Read all the data at a particular state/district/court and mark it done when finished.
        data = read_table_data_and_links(base_url)
        try:
            for state in data:
                if state['url'] != '':
                    state['districts'] = read_table_data_and_links(ROOT_URL + state['url'][2:])
                    for district in state['districts']:
                        if district['url'] != '':
                            district['courts'] = read_table_data_and_links(ROOT_URL + district['url'][2:])
                            for court in district['courts']:
                                if court['url'] != '':
                                    court['judges'] = read_table_data_and_links(
                                        ROOT_URL + court['url'][2:] + '&captchaValid=valid')
                                    counter += len(court['judges'])
                                    logger.info('Scraped {} judges'.format(counter))
                                court['done'] = True
                        district['done'] = True
                state['done'] = True
            write_judge_data_to_csv(output_file, data)

        # Error handling - because when something breaks, we want to know why.
        except requests.ConnectionError as e:
            logger.error(
                "OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.")
            logger.error(str(e))
        except requests.Timeout as e:
            logger.error("OOPS!! Timeout Error")
            logger.error(str(e))
        except requests.RequestException as e:
            logger.error("OOPS!! General Error")
            logger.error(str(e))
        except KeyboardInterrupt:
            logger.info("Someone closed the program")
            shutdown_by_keyboard_interrupt = True
        finally:
            # Dump all the parsed data to a JSON file on error in order to allow resuming the scraper from the same
            # point after a crash.
            with open('./cache.json', 'w+') as cache_file:
                json.dump(data, cache_file, sort_keys=True, indent=4)
            with open('counter.pkl', 'wb') as counter_cache:
                pickle.dump(counter, counter_cache)
            if not shutdown_by_keyboard_interrupt:
                scrape_summary(output_file, category)

    else:
        # If there is an incomplete scrape, load the cache file and resume from wherever we stopped off.
        with open('counter.pkl', 'rb') as counter_cache_to_load:
            counter = pickle.load(counter_cache_to_load)
        with open('./cache.json', 'r') as cache_file:
            data = json.load(cache_file)
            try:
                # At each level (state/district/court), check if all items are marked done. If not, figure out which
                # level is incomplete and fill it in.
                for state in data:
                    if (len(state.get('district', [])) > 0 and state.get('districts', [{}])[0].get('error_flag', False)) or not state.get('done', False):
                        if state['url'] != '':
                            if len(state.get('district', [])) > 0 and state.get('districts', [{}])[0].get(
                                    'error_flag', False):
                                logger.info('Attempting to fix entry with URL {}'.format(state['url']))
                            temp_districts = read_table_data_and_links(ROOT_URL + state['url'][2:])
                            if len(temp_districts) > len(state.get('districts', [])):
                                state['districts'] = temp_districts

                            for district in state.get('districts', []):
                                if (len(district.get('courts', [])) > 0 and district.get('courts', [{}])[0].get(
                                        'error_flag', False)) or not district.get('done',
                                                                                 False):
                                    if district['url'] != '':
                                        if len(district.get('courts', [])) > 0 and district.get('courts', [{}])[0]\
                                                .get('error_flag', False):
                                            logger.info('Attempting to fix entry with URL {}'.format(district['url']))
                                        temp_courts = read_table_data_and_links(ROOT_URL + district['url'][2:])
                                        if len(temp_courts) > len(district.get('courts', [])):
                                            district['courts'] = temp_courts

                                        for court in district.get('courts', []):
                                            if (len(court.get('judges', [])) > 0 and court.get('judges', [{}])[0].get('error_flag', False)) or \
                                                    not court.get('done', False):
                                                if court['url'] != '':
                                                    if len(court.get('judges', [])) > 0 and court.get('judges', [{}])[0].get('error_flag', False):
                                                        logger.info('Attempting to fix entry with URL {}'.format(
                                                            court['url']))
                                                    court['judges'] = read_table_data_and_links(
                                                        ROOT_URL + court['url'][2:] + '&captchaValid=valid')
                                                    counter += len(court['judges'])
                                                    logger.info('Scraped {} judges'.format(counter))
                                            if len(court.get('judges', [])) > 0 and not court.get('judges', [{}])[0].get('error_flag', False):
                                                court['done'] = True
                                    if len(district.get('courts', [])) > 0 and not district.get('courts', [{}])[0].get('error_flag', False):
                                        district['done'] = True
                        if len(state.get('district', [])) > 0 and not state.get('districts', [{}])[0].get('error_flag', False):
                            state['done'] = True
                write_judge_data_to_csv(output_file, data)

            # Ye Olde Crash Resumption and Error Handling Code
            except requests.ConnectionError as e:
                logger.error(
                    "OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.")
                logger.error(str(e))
            except requests.Timeout as e:
                logger.error("OOPS!! Timeout Error")
                logger.error(str(e))
            except requests.RequestException as e:
                logger.error("OOPS!! General Error")
                logger.error(str(e))
            except KeyboardInterrupt:
                logger.info("Someone closed the program")
                shutdown_by_keyboard_interrupt = True
            except Exception:
                logger.error('Unhandled exception!')
                traceback.print_exc()
            finally:
                with open('./cache.json', 'w+') as cache_file:
                    json.dump(data, cache_file, sort_keys=True, indent=4)
                with open('counter.pkl', 'wb') as counter_cache:
                    pickle.dump(counter, counter_cache)
                if not shutdown_by_keyboard_interrupt:
                    scrape_summary(output_file, category)


if __name__ == '__main__':
    # Setting up the logger
    logger = logging.getLogger('njdg_v1_scraper')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('scraper.log')
    ch = logging.StreamHandler()
    fh.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    parser = argparse.ArgumentParser(description='Scrape data from the NJDG database.')
    parser.add_argument('--output_file', default='output.csv', help='The file to write scraped data to.')

    get_to_cases_root()  # Getting past the captcha and to the NJDG top-level page.

    args = parser.parse_args()

    scrape_summary(args.output_file)
