"""
Announcements PairList provider

Provides dynamic pair list based on exchanges announcements.

Supported Announcement exchanges:
- Binance
- Kucoin

"""
import random
import time
from abc import abstractmethod
from bs4 import BeautifulSoup
from cachetools import cached
from cachetools.ttl import TTLCache
from datetime import datetime, timedelta
from requests import get
from typing import Any, Dict, List, Optional

import logging
import re
import pytz

import pandas as pd
from sqlalchemy.exc import ProgrammingError

from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.plugins.pairlist.IPairList import IPairList
from freqtrade.persistence import announcements

logger = logging.getLogger(__name__)


def get_token_from_pair(pair: str, index: int = 0) -> str:
    return pair.split('/')[index]


class AnnouncementMixin:

    REFRESH_PERIOD: int
    TOKEN_COL: str
    ANNOUNCEMENT_COL: str

    def __init__(self, whitelist_hours: int, *args, **kwargs):
        self.whitelist_hours = whitelist_hours

    @abstractmethod
    def update_announcements(self, *args, **kwargs) -> pd.DataFrame:
        """
        Called by AnnouncementsPairList.
        It must be implemented: it must update database with fresh announcements and return updated dataframe.
        *args, **kwargs are defined for internal scopes. No arguments will be passed.
        """


class BinanceAnnouncement(AnnouncementMixin):
    BINANCE_CATALOG_ID = 48
    BINANCE_BASE_URL = "https://www.binance.com/"
    BINANCE_ANNOUNCEMENT_URL = BINANCE_BASE_URL + 'en/support/announcement/'
    BINANCE_ANNOUNCEMENTS_URL = BINANCE_ANNOUNCEMENT_URL + "c-{}?navId={}#/{}"
    BINANCE_API_QUERY = "query?catalogId={}&pageNo={}&pageSize={}"
    BINANCE_API_URL = BINANCE_BASE_URL + "bapi/composite/v1/public/cms/article/catalog/list/" + BINANCE_API_QUERY

    # css classes
    BINANCE_DATETIME_CSS_CLASS = 'css-17s7mnd'

    # token info
    BINANCE_TOKEN_REGEX = re.compile(r'\((\w+)\)')
    BINANCE_KEY_WORDS = ['list', ]  # 'token sale', 'perpetual', 'open trading',

    # 'opens trading',  'defi', 'uniswap', 'airdrop'
    BINANCE_KEY_WORDS_BLACKLIST = ['listing postponed', 'futures', 'leveraged']

    REFRESH_PERIOD = 3

    # storage
    COLS = ['Token', 'Text', 'Link', 'Datetime discover', 'Datetime announcement']

    TOKEN_COL = 'Token'
    ANNOUNCEMENT_COL = 'Datetime announcement'

    table_name = 'binanceannouncements'
    _df: Optional[pd.DataFrame] = None

    def __init__(self, *args, **kwargs):
        self.db_path = kwargs.pop('db_path', "user_data/data/BinanceAnnouncements_announcements.csv")
        super().__init__(*args, **kwargs)

    def update_announcements(self, page_number=1, page_size=10, max_page=100, history=False) -> pd.DataFrame:
        headers = {
            "Cache-Control": "max-age=0",
        }
        response = None
        url = self.get_api_url(page_number, page_size)

        if history:
            # recursive updating
            return [self.update_announcements(
                page, page_size, history=False
            ) for page in reversed(range(2, 56))][-1]

        try:
            now = datetime.now(tz=pytz.utc)
            df = self.get_df(refresh=True)

            try:
                response = get(url, headers=headers)
            except Exception as e:
                raise TemporaryError(f"Binance url ({url}) is not available. Original Exception: {e}")

            if not history:
                while 'Age' in (response.headers or {}):
                    try:
                        url = self.get_api_url(random.randint(1, max_page), page_size)
                        response = get(url, headers=headers)
                    except:
                        break
                    time.sleep(0.5)

            if response.status_code != 200:
                raise TemporaryError(f"Invalid response from url: {url}.\n"
                                     f"Status code: {response.status_code}\n"
                                     f"Content: {response.content.decode()}")

            logger.info("Updating from Binance...")
            updated_list = []
            articles = response.json()['data']['articles'] or []
            for article in articles:
                article_link = self.get_announcement_url(article['code'])
                article_text = article['title']

                tokens = self._get_tokens(article_text)

                if not tokens:
                    token = self.get_token_by_article(article_link, raise_exceptions=False)
                    if token:
                        tokens = [token]

                for token in tokens:
                    if token:
                        updated_list.extend(
                            self._get_new_data(
                                now=now,
                                token=token,
                                key_words=self.BINANCE_KEY_WORDS,
                                article_text_lower=article_text.lower(),
                                article_link=article_link,
                                article_text=article_text,
                                df=df
                            )
                        )

            if df is not None:
                df = df.append(pd.DataFrame(updated_list, columns=self.COLS), ignore_index=True)
            else:
                df = pd.DataFrame(updated_list, columns=self.COLS)

            if updated_list:
                logger.info(f"Adding tokens to database: {[upd[0] for upd in updated_list]}")
                self._save_df(df)
            return df

        except TemporaryError as e:
            # exception handled, re-raise
            logger.error(e)
            raise e

        except Exception as e:
            # exception not handled raise OperationalException
            logger.error(e)
            raise OperationalException(f"Some errors occurred processing Binance data. "
                                       f"Url: {url}.\n"
                                       f"Status code: {response.status_code if response else None}\n"
                                       f"Content: {response.content.decode() if response else None}\n"
                                       f"Exception: {e}")

    def _get_new_data(self, now, token, key_words, article_text_lower, article_link, article_text, df=None):
        have_df = df is not None
        updated_list = []
        for item in key_words:
            conditions_buy = (
                (item in article_text_lower)  # key matched
                and (
                    not have_df  # is first time data
                    or not (token is None or token in df[self.TOKEN_COL].values)  # not an existing or null token
                )
            )
            if conditions_buy:
                if any(i in article_text_lower for i in self.BINANCE_KEY_WORDS_BLACKLIST):
                    logger.debug(f'BLACKLISTED: "{article_text}", skip.')
                    continue

                if token:
                    logger.info(f'Found new announcement: "{article_text}". Token: {token}.')
                    updated_list.append(
                        [token, article_text, article_link, now, self.get_datetime_announcement(article_link)]
                    )
        return updated_list

    def _get_tokens(self, text: str):
        return self.BINANCE_TOKEN_REGEX.findall(text)

    def get_df(self, refresh=False):
        """Get Dataframe from CSV file or from Database"""
        if self._df is None or refresh:
            self.load_csv_db() if self.db_path.endswith('csv') else self.load_db()
        return self._df

    def load_csv_db(self):
        try:
            self._df = pd.read_csv(self.db_path, parse_dates=['Datetime discover', 'Datetime announcement'])
        except FileNotFoundError:
            pass

    def load_db(self):
        try:
            self._df = announcements.get_df(self.db_path, self.table_name)
        except ProgrammingError:
            pass

    def save_db(self):
        announcements.save_df(self._df, self.db_path, self.table_name)

    def _save_df(self, df: pd.DataFrame):
        self._df = df.sort_values(by='Datetime announcement')
        self._df.to_csv(self.db_path, index=False) if self.db_path.endswith('csv') else self.save_db()

    def get_datetime_announcement(self, announcement_url: str):
        response = get(announcement_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        for el in soup.find_all(class_=self.BINANCE_DATETIME_CSS_CLASS):
            try:
                return datetime.strptime(el.text, '%Y-%m-%d %H:%M').replace(tzinfo=pytz.utc)
            except Exception as e:
                logger.error(e)
                continue

        msg = f"Cannot find datetime_announcement in announcement_url: {announcement_url}. " \
              f"Probably a CSS class change."

        exc = OperationalException(msg)
        logger.error(exc)

    @staticmethod
    def get_token_by_article(article_link, raise_exceptions: bool = True):
        # TODO
        if raise_exceptions:
            raise ValueError("Token not found")

    def get_announcement_url(self, code: str) -> str:
        return "".join([self.BINANCE_ANNOUNCEMENT_URL, code])

    @property
    def announcements_url(self) -> str:
        return self.BINANCE_ANNOUNCEMENTS_URL.format(*[self.BINANCE_CATALOG_ID for _ in range(3)])

    def get_api_url(self, page_number: int = 1, page_size: int = 10) -> str:
        return self.BINANCE_API_URL.format(self.BINANCE_CATALOG_ID, page_number, page_size)


class KucoinAnnouncement(AnnouncementMixin):

    CATALOG = 'listing'
    BASE_URL = "https://www.kucoin.com/_api/cms/articles"
    ANNOUNCEMENT_URL = 'https://www.kucoin.com/news/'
    API_QUERY = "?page={}&pageSize={}&category={}&c=d3a4f4dcc33844fbbabc3f9cc0abb3cf&lang=en_US"
    API_URL = BASE_URL + API_QUERY

    # token info
    TOKEN_REGEX = re.compile(r'\((\w+)\)')

    # 'opens trading',  'defi', 'uniswap', 'airdrop'
    KEY_WORDS_BLACKLIST = ['listing postponed', 'futures', 'leveraged']

    REFRESH_PERIOD = 3

    # storage
    COLS = ['Token', 'Text', 'Link', 'Datetime discover', 'Datetime announcement']

    TOKEN_COL = 'Token'
    ANNOUNCEMENT_COL = 'Datetime announcement'

    table_name = 'kucoinannouncements'

    _df: Optional[pd.DataFrame] = None

    def __init__(self, *args, **kwargs):
        self.db_path = kwargs.get('db_path', "user_data/data/KucoinAnnouncements_announcements.csv")
        super().__init__(*args, **kwargs)

    def update_announcements(self, page_number=1, page_size=10, max_page=100, history=False) -> pd.DataFrame:
        headers = {
            "Cache-Control": "max-age=0",
        }
        response = None
        url = self.get_api_url(page_number, page_size)

        if history:
            # recursive updating
            return [self.update_announcements(
                page, page_size, history=False
            ) for page in reversed(range(2, 56))][-1]

        try:
            now = datetime.now(tz=pytz.utc)
            df = self.get_df(refresh=True)

            try:
                response = get(url, headers=headers)
            except Exception as e:
                raise TemporaryError(f"Kucoin url ({url}) is not available. Original Exception: {e}")

            if response.status_code != 200:
                raise TemporaryError(f"Invalid response from url: {url}.\n"
                                     f"Status code: {response.status_code}\n"
                                     f"Content: {response.content.decode()}")

            logger.info("Updating from Kucoin...")
            updated_list = []
            articles = response.json()['items'] or []
            for article in articles:
                article_link = self.get_announcement_url(article['path'])
                article_text = article['title']

                tokens = self._get_tokens(article_text)

                for token in tokens:
                    if token:
                        updated_list.extend(
                            self._get_new_data(
                                data=article,
                                now=now,
                                token=token,
                                article_text_lower=article_text.lower(),
                                article_link=article_link,
                                article_text=article_text,
                                df=df
                            )
                        )

            if df is not None:
                df = df.append(pd.DataFrame(updated_list, columns=self.COLS), ignore_index=True)
            else:
                df = pd.DataFrame(updated_list, columns=self.COLS)

            if updated_list:
                logger.info(f"Adding tokens to database: {[upd[0] for upd in updated_list]}")
                self._save_df(df)
            return df

        except TemporaryError as e:
            # exception handled, re-raise
            logger.error(e)
            raise e

        except Exception as e:
            # exception not handled raise OperationalException
            logger.error(e)
            raise OperationalException(f"Some errors occurred processing KUCOIN data. "
                                       f"Url: {url}.\n"
                                       f"Status code: {response.status_code if response else None}\n"
                                       f"Content: {response.content.decode() if response else None}\n"
                                       f"Exception: {e}")

    def _get_new_data(self, data, now, token, article_text_lower, article_link, article_text, df=None):
        updated_list = []
        conditions_buy = (
            df is None  # is first time data
            or not (token is None or token in df[self.TOKEN_COL].values)  # not an existing or null token
        )
        if conditions_buy:
            if any(i in article_text_lower for i in self.KEY_WORDS_BLACKLIST):
                logger.debug(f'BLACKLISTED: "{article_text}", skip.')
                return updated_list

            if token:
                logger.info(f'Found new announcement: "{article_text}". Token: {token}.')
                updated_list.append(
                    [token, article_text, article_link, now, self.get_datetime_announcement(data)]
                )
        return updated_list

    def _get_tokens(self, text: str):
        return self.TOKEN_REGEX.findall(text)

    def get_df(self, refresh=False):
        """Get Dataframe from CSV file or from Database"""
        if self._df is None or refresh:
            self.load_csv_db() if self.db_path.endswith('csv') else self.load_db()
        return self._df

    def load_csv_db(self):
        try:
            self._df = pd.read_csv(self.db_path, parse_dates=['Datetime discover', 'Datetime announcement'])
        except FileNotFoundError:
            pass

    def load_db(self):
        try:
            self._df = announcements.get_df(self.db_path, self.table_name)
        except ProgrammingError:
            pass

    def save_db(self):
        announcements.save_df(self._df, self.db_path, self.table_name)

    def _save_df(self, df: pd.DataFrame):
        self._df = df.sort_values(by='Datetime announcement')
        self._df.to_csv(self.db_path, index=False) if self.db_path.endswith('csv') else self.save_db()

    @staticmethod
    def get_datetime_announcement(data: dict):
        return datetime.strptime(data['publish_at'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc) - timedelta(hours=8)

    def get_announcement_url(self, path: str) -> str:
        # https://www.kucoin.com/news/en-aragon-ant-gets-listed-on-kucoin
        return "".join([self.ANNOUNCEMENT_URL, path])

    def get_api_url(self, page_number: int = 1, page_size: int = 10) -> str:
        return self.API_URL.format(page_number, page_size, self.CATALOG)


class AnnouncementsPairList(IPairList):
    """
    Announcement pair list.
    Return pairs that are listed on the selected exchange for less than some specified hours (default 24)
    """
    # sleep at least 3 seconds every request by default
    REFRESH_PERIOD = 3
    STATIC = False

    def __init__(self, exchange, pairlistmanager,
                 config: Dict[str, Any], pairlistconfig: Dict[str, Any],
                 pairlist_pos: int) -> None:

        super().__init__(exchange, pairlistmanager, config, pairlistconfig, pairlist_pos)

        if 'exchanges' not in self._pairlistconfig or not self._pairlistconfig['exchanges']:
            raise OperationalException(
                '`exchanges` not specified. Please check your configuration '
                'for "pairlist.config.exchanges"')

        self._stake_currency = config['stake_currency']
        self._pair_exchanges = list(self._pairlistconfig['exchanges'].keys())
        self._pair_cache = TTLCache(maxsize=1, ttl=self._pairlistconfig.get('refresh_period', self.REFRESH_PERIOD))
        self.pair_exchanges = []

        try:
            for pair_exchange in self._pair_exchanges:
                pair_exchange_kwargs = self._pairlistconfig['exchanges'][pair_exchange].get('exchange_kwargs', {})
                pair_exchange_kwargs['whitelist_hours'] = self._pairlistconfig['exchanges'][pair_exchange].get('hours', 24)
                self.pair_exchanges.append(self._init_pair_exchange(pair_exchange, config, **pair_exchange_kwargs))
        except AssertionError as e:
            raise OperationalException(f"Announcement class is improperly configured. Exception: {e}") from e

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        If no Pairlist requires tickers, an empty Dict is passed
        as tickers argument to filter_pairlist
        """
        # Set at False.
        # This PairList needs tickers but not with standard frequency.
        # So we take it caching for more time. see _get_cached_tickers. ttl=3600
        return False

    @cached(TTLCache(maxsize=1, ttl=3600))
    def _get_cached_tickers(self):
        return self._exchange.get_tickers()

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages
        """
        return f"{self.name} - {', '.join(self._pair_exchanges)} announced pairs."

    def gen_pairlist(self, tickers: Dict) -> List[str]:
        """
        Generate the pairlist
        :param tickers: Tickers (from exchange.get_tickers()). May be cached.
        :return: List of pairs
        """
        pairlist = self._pair_cache.get('pairlist')
        if pairlist:
            # Item found - no refresh necessary
            return pairlist.copy()
        else:
            tickers = self._get_cached_tickers()
            filtered_tickers = [
                v for k, v in tickers.items()
                if (self._exchange.get_pair_quote_currency(k) == self._stake_currency)
             ]

            pairlist = [s['symbol'] for s in filtered_tickers]
            pairlist = self.filter_pairlist(pairlist, tickers)
            self._pair_cache['pairlist'] = pairlist.copy()

        return pairlist

    def filter_pairlist(self, pairlist: List[str], tickers: Dict) -> List[str]:
        """
        Filters and sorts pairlist and returns the whitelist again.
        Called on each bot iteration - please use internal caching if necessary
        :param pairlist: pairlist to filter or sort
        :param tickers: Tickers (from exchange.get_tickers()). May be cached.
        :return: new whitelist
        """
        filtered_pairlist = []
        for pair_exchange in self.pair_exchanges:
            df = pair_exchange.get_df(refresh=True) if self.STATIC else pair_exchange.update_announcements(
                page_size=random.randint(1, 100)  # randomize page size to avoid binance caching document page
            )
            df = df[df[pair_exchange.ANNOUNCEMENT_COL] > (datetime.now(tz=pytz.utc) - timedelta(
                hours=pair_exchange.whitelist_hours
            ))]

            if df.empty:
                continue

            pairs = [get_token_from_pair(v, 0) for v in pairlist]
            df = df[df[pair_exchange.TOKEN_COL].isin(pairs)]

            filtered_pairlist += [f"{token}/{self._stake_currency}" for token in df[pair_exchange.TOKEN_COL]]
        return list(set(filtered_pairlist))

    @staticmethod
    def _init_pair_exchange(pair_exchange, config, **pair_exchange_kwargs):
        pair_exchange_kwargs['db_path'] = config.get('ba_database_uri', None)

        if pair_exchange == 'binance':
            exchange = BinanceAnnouncement(**pair_exchange_kwargs)
        elif pair_exchange == 'kucoin':
            exchange = KucoinAnnouncement(**pair_exchange_kwargs)
        else:
            raise OperationalException(f'Exchange `{pair_exchange}` is not supported yet')

        assert hasattr(exchange, 'update_announcements'), '`update_announcements` method is required'
        assert hasattr(exchange, 'TOKEN_COL'), '`TOKEN_COL` attribute is required'
        assert hasattr(exchange, 'ANNOUNCEMENT_COL'), '`ANNOUNCEMENT_COL` attribute is required'
        assert exchange.TOKEN_COL is not None
        assert exchange.ANNOUNCEMENT_COL is not None
        return exchange

    def _validate_pair(self, pair: str, ticker: Dict[str, Any]) -> bool:
        return NotImplemented


class StaticAnnouncementsPairList(AnnouncementsPairList):
    STATIC = True
