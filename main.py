""" project1.py

Complete the sections below marked with '<COMPLETE THIS PART>'
         
"""
import os
import datetime as dt
import numpy as np
import pandas as pd


# SRCDIR is the folder containing all the data:
# - `<tic>_prc.dat` 
# - `<tic>_rec.csv` 
# - ff_dailyc.csv` 
#
SRCDIR = r'C:\Users\steve\PycharmProjects\yfinance_downloader\z5194925\data'

# TICKERS is the location of the TICKERS.txt file 
TICKERS = r'C:\Users\steve\PycharmProjects\yfinance_downloader\z5194925\TICKERS.txt'

# FF_CSV is the location of the ff_daily.csv  file 
FF_CSV = r'C:\Users\steve\PycharmProjects\yfinance_downloader\z5194925\data\ff_daily.csv'


# ---------------------------------------------------------------------------- 
#   Modify these variables as specified by the README.txt file
# ---------------------------------------------------------------------------- 

# NOTE: 
# - SRC_COLS must be a list
# - The order of the elements must match the order of the columns specified
#   in the README.txt file.
#
SRC_COLS = ['Volume', 'Open', 'Date', 'Adj Close', 'Close', 'Low']

# NOTE:
# - SRC_COL_DTYPES must be a dict
# - The keys should be the column names, the values should be their dtype, as
#   specified in the README.txt file.
#
d_types = ['int64', 'float64', 'datetime64', 'float64', 'float64', 'float64']
SRC_COL_DTYPES = dict(zip(SRC_COLS, d_types))

# NOTE: 
# - SRC_COL_WIDTHS should be a dict
# - The keys should be the column names, the values should be the width of
#   that field in the DAT file. These should match the widths defined in the
#   README.txt file.
#
col_width = [7, 12, 11, 15, 12, 6]
SRC_COL_WIDTHS = dict(zip(SRC_COLS, col_width))


# ---------------------------------------------------------------------------- 
#   Function get_tics
# ---------------------------------------------------------------------------- 
def get_tics(pth):
    """ Reads a file with tickers (one per line) and returns a list
    of formatted tickers (see the notes below).

    Parameters
    ----------
    pth : str
        Location of the TICKERS.txt file

    Returns
    -------
    list
        List where each element represents a ticker (formatted as below)

    Notes
    -----
    - The tickers returned must conform with the following rules:
        - All characters are in lower case
        - There are no spaces
        - The list contains no empty/blank tickers
    """
    # <COMPLETE THIS PART>
    x = []
    with open(pth) as file:
        for l in file:
            if len(l.split()) == 0:
                continue
            x.append(l.strip().lower())
        x = list(dict.fromkeys(x))
    return x



# ---------------------------------------------------------------------------- 
#   Function dat_to_df *MIGHT NEED TO PARSE IN src_col_dtypes when reading)
# ---------------------------------------------------------------------------- 
def dat_to_df(pth, 
        src_cols, 
        src_col_dtypes,
        src_col_widths,
        ):
    """ This function creates a dataframe with the contents of a DAT file
    containing stock price information for a given ticker. 
    

    Parameters
    ----------
    pth : str
        Location of the DAT file containing price information (i.e. some
        `<tic>_prc.dta`)

    src_cols : list
        List containing the column names in the order they appear in each
        source DAT file. The order of columns must match the order specified
        in the README.txt file

    src_col_dtypes : dict
        A dictionary mapping each column name in `src_cols` to its data type,
        as it appears in the `README.txt` file.

    src_col_widths : dict
        A dictionary mapping each column name in `src_cols` to its column
        width as it appears in the `README.txt` file.


    Returns
    -------
    df 
        A Pandas dataframe containing the stock price information from the DAT
        file in `pth` This dataframe must meet the following criteria:

        
        - df.index: DatetimeIndex with dates, matching the dates contained in
          the DAT file. The labels in the index must be datetime objects.

        - df.columns: each column label will be a column in `src_cols`, with the
          exception of 'Date'. The order of the column labels in this index
          must match the order specified in the README.txt file

        - Each series inside this dataframe (must correspond to a column in
          the README.txt file (with the exception of 'Date'). The datatype of
          each series must match the data type specified in the README.txt
          file.

    """
    # <COMPLETE THIS PART>
    df = pd.read_fwf(filepath_or_buffer=pth, widths=list(src_col_widths.values()), header=None)
    df.columns = src_cols
    df = df.astype(src_col_dtypes)
    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index)
    return df




# ---------------------------------------------------------------------------- 
#   Function mk_prc_df
# ---------------------------------------------------------------------------- 
def mk_prc_df(
        tickers, 
        srcdir,
        src_cols, 
        src_col_dtypes,
        src_col_widths,
        ):
    """ This function creates a dataframe from the information found in
    a DAT file located at `pth`

    Parameters
    ----------
    tickers : list
        List of tickers in the order they appear in the TICKERS.txt file

    srcdir : str
        Directory containing the source files:
        - <tic>_prc.dat for each <tic> in TICKERS.txt
        - <tic>_rec.csv for each <tic> in TICKERS.txt

    src_cols : list
        List containing the column names in the order they appear in each
        source DAT file. The order of columns must match the order specified
        in the README.txt file

    src_col_dtypes : dict
        A dictionary mapping each column name in `src_cols` to its data type,
        as it appears in the `README.txt` file.

    src_col_widths : dict
        A dictionary mapping each column name in `src_cols` to its column
        width as it appears in the `README.txt` file.


    Returns
    -------
    df 
        A Pandas dataframe containing the adjusted closing price for each
        stock identified in the TICKERS.txt file. This dataframe must match
        the following criteria:
        
        - df.index: DatetimeIndex with dates. 

        - df.columns: each column label will contain the ticker code
          (in lower case). The number of columns in this dataframe must correspond
          to the number of tickers in the `TICKERS.txt` file above. The order
          of the columns must match the order of the tickers in `TICKERS.txt`.

        - The data inside each column (i.e. series) will contain the closing
          prices included in each DAT file (the Adj Close column). All valid
          closing prices (for tickers in TICKERS.txt) must be included in this
          dataframe. If the closing price for a ticker is not available in the
          DAT file, it will take a NaN value.

    Notes
    -----
    - This function will call the `dat_to_df` function for each ticker

    - The output of this function is a dataframe that looks like this (the
      contents of the df below are for illustration purposes only and will
      **not** necessarily represent the actual contents of the dataframe you
      create):

                          aapl ...       tsla
        Date 
        1980-12-12    0.101261 ...        NaN
        ...                    ...        ...
        2020-10-05  116.500000 ... 425.679993
        2020-10-06  113.160004 ... 413.980011
        2020-10-07  115.080002 ... 425.299988
        2020-10-08  114.970001 ... 425.920013
        2020-10-09  116.970001 ... 434.000000

    """
    # <COMPLETE THIS PART>
    tickers = list(dict.fromkeys(tickers))
    for tic in tickers:
        tic_pth = os.path.join(srcdir, tic.lower() + '_prc.dat')
        df = dat_to_df(tic_pth, src_cols, src_col_dtypes, src_col_widths)
        adj_close_series = df['Adj Close']
        if tickers.index(tic) == 0:
            final_series = adj_close_series
            continue
        else:
            final_series = pd.concat([final_series, adj_close_series], axis=1)
    final_series.columns = [x.lower() for x in tickers]
    return final_series



# ---------------------------------------------------------------------------- 
#   Function mk_aret_df
# ---------------------------------------------------------------------------- 
def mk_aret_df(prc_df):
    """ Creates a dataframe with abnormal returns given the price information
    contained in the `prc_df`

    Parameters
    ----------
    prc_df : dataframe
        Dataframe produced by the function `mk_prc_df` above

    Returns
    -------
    dataframe
        Dataframe with abnormal returns for each ticker. Abnormal returns are
        computed by subtracting the market return from that stock's returns. 

        - df.index: DatetimeIndex with dates, matching the dates contained in
          the DAT file. The labels in the index must be datetime objects.

        - df.columns: each column label will be a column in `src_cols`, with the
          exception of 'Date'. The order of the column labels in this index
          must match the order specified in the README.txt file

        - Each series inside this dataframe contains the abnormal return for
          each ticker in TICKERS.txt. 

    Notes
    -----

    The output of this function is a dataframe that looks like this (the
    contents of the df below are for illustration purposes only and will
    **not** necessarily represent the actual contents of the dataframe you
    create):

                        aapl      tsla
        Date                          
        1980-12-12       NaN       NaN
        1980-12-15 -0.052684       NaN
        1980-12-16 -0.079905       NaN
        1980-12-17  0.010143       NaN
        1980-12-18  0.025475       NaN
        ...              ...       ...
        2020-08-25 -0.011804  0.000938
        ...              ...       ...

    """
    # --------------------------------------------------------     
    #   Create returns
    #   ret_df must be similar to this: 
    # 
    #                    aapl      tsla
    #    Date                          
    #    1980-12-12       NaN       NaN
    #    1980-12-15 -0.052174       NaN
    #    1980-12-16 -0.073395       NaN
    #    1980-12-17  0.024753       NaN
    #    1980-12-18  0.028985       NaN
    #    ...              ...       ...
    #    2020-10-12  0.063521  0.019124
    #    ...              ...       ...
    # -------------------------------------------------------- 
    ret_df = prc_df.pct_change()


    # --------------------------------------------------------     
    #   Load FF mkt rets (do not change this part)
    # -------------------------------------------------------- 
    ff_df = pd.read_csv(FF_CSV, index_col='Date', parse_dates=['Date'])
    aret_df = ret_df.join(ff_df.mkt, how='inner')

    # --------------------------------------------------------     
    #   Create abnormal rets
    # -------------------------------------------------------- 
    for tic in ret_df.columns:
        aret_df.loc[:, tic] = aret_df.loc[:, tic] - aret_df.loc[:, 'mkt']
    del aret_df['mkt']
    return aret_df



# ---------------------------------------------------------------------------- 
#   Function read_rec_csv
# ---------------------------------------------------------------------------- 
def read_rec_csv(tic):
    """ This function will read the CSV file containing the recommendations
    for the ticker `tic` and produce a dataframe with the characteristics
    described below.

    Parameters
    ----------
    tic : str
        Ticker in lower case characters and without spaces

    Returns
    -------
    df 
        A Pandas dataframe matching the following criteria:

        - df.index : is a DatetimeIndex with the date and timestamp of the
            recommendation.

        - df.columns: index with labels ['event_day', 'firm', 'action'], in
          this order, where
            - 'event_day': String representing the date of the recommendation
              (no timestamp) following the format in the Notes section below
            - 'firm': Firm the analyst belongs to (source column 'Firm').
            - 'action': Information from the source column 'Action'
    
    Notes
    -----

    The output of this function is a dataframe that looks like this (the
    contents of the df below are for illustration purposes only and will
    **not** necessarily represent the actual contents of the dataframe you
    create):


        |                     | event_day  | firm            | action |
        | index (datetime64)  |            |                 |        |
        |---------------------+------------+-----------------+--------|
        | 2012-02-16 13:53:00 | 2012-02-16 | Wunderlich      | down   |
        | 2012-03-26 07:31:00 | 2012-03-26 | Wunderlich      | up     |
        | 2012-09-17 05:46:00 | 2012-09-17 | Morgan Stanley  | main   |
        | 2013-02-21 06:53:02 | 2013-02-21 | Bank of America | init   |
        | 2020-07-28 09:57:21 | 2020-07-28 | Bernstein       | down   |
        | 2020-07-28 10:50:12 | 2020-07-28 | Bernstein       |        |
        | 2020-08-14 09:19:00 | 2020-08-14 | Morgan Stanley  | up     |

    The output of df.info() should look like this:

        DatetimeIndex: ... entries, ...
        Data columns (total 3 columns):
         #   Column     Non-Null Count  Dtype 
        ---  ------     --------------  ----- 
         0   event_day  132 non-null    object
         1   firm       132 non-null    object
         2   action     132 non-null    object
        dtypes: object(3)


    """
    # <COMPLETE THIS PART>
    csv_path = os.path.join(SRCDIR, tic + '_rec.csv')
    df = pd.read_csv(csv_path, usecols=['Date', 'Firm', 'Action'], parse_dates=['Date'])
    df['event_day'] = df['Date'].dt.date
    # change index label
    df.set_index('Date', inplace=True)
    df.index.name = 'index'
    df.columns = ['firm', 'action', 'event_day']
    cols = ['event_day', 'firm', 'action']
    return df[cols]



def proc_rec_df(rec_df):
    """ This function takes a dataframe with the recommendations for a given
    ticker and performs the following operations **in this order**:

    1. Keep only the top 30 firms (in terms of number of recommendations over
       the entire sample period) for this ticker (see Notes below).

       *Just chose the 30 most popular firms FIRST*

    2. Keep only recommendations that represent either an upgrade or a
       downgrade (that is, the values of `rec_df['action']` are either 'up' or
       'down'). If there are no observations that match this criterion, this
       function will return an empty dataframe with the index and columns
       specified in the Returns section below.

       *Select top 30 firms (from step 1)*
       *Let action be only up or down*

    3. Return the dataframe as described in the Returns section below.


    Parameters
    ----------
    rec_df : dataframe
        Dataframe produced by the function `read_rec_csv` created above.
    

    Returns
    -------
    df 
        A Pandas dataframe with the same structure as `rec_df` but only
        including upgrades and downgrades:

        - df.index : index of the same type as rec_df.index, but not
          necessarily of the same length.

        - df.columns : columns as in rec_df.columns

        - df['action']: if dataframe is not empty, this column should only
          contain values 'up' or 'down'.

    Notes
    -----
    - To select the top 30 firms:

        1. Count the number of observations for each individual value of the
        column 'firm'.
        2. Sort the result by counts in descending order.
        3. For each count value, sort the firms in the group alphabetically
        4. Keep the first 30 firms only. 

      The procedure above means that if there is a tie for the 30th highest
      count, the firm name will be used when deciding which firms to keep
      (alphabetical priority).
        
        

    - The output of this function is a dataframe that looks like this (the
      contents of the df below are for illustration purposes only and will
      **not** represent the actual contents of the dataframe you create
      necessarily. In addition, the actual dataframe returned could be empty):


        |                     | event_day  | firm           | action |
        | index (datetime64)  |            |                |        |
        |---------------------+------------+----------------+--------|
        | 2012-02-16 13:53:00 | 2012-02-16 | Wunderlich     | down   |
        | 2012-03-26 07:31:00 | 2012-03-26 | Wunderlich     | up     |
        | 2020-07-28 09:57:21 | 2020-07-28 | Bernstein      | down   |
        | 2020-08-14 09:19:00 | 2020-08-14 | Morgan Stanley | up     |


    """
    # -------------------------------------------------------- 
    #   Get top 30 firms
    # -------------------------------------------------------- 
    count_ser = rec_df.groupby('firm').size()
    df_count = pd.DataFrame({'firm': count_ser.index, 'count': count_ser.values})
    df_count = df_count.sort_values(by=['count', 'firm'], ascending=[False, True])
    # -------------------------------------------------------- 
    #   Subset the DF to include only these firms
    # -------------------------------------------------------- 
    firm_ser = df_count.iloc[0:30, 0]
    firm_list = firm_ser.values.tolist()
    rec_df = rec_df[rec_df['firm'].isin(firm_list)]

    # -------------------------------------------------------- 
    #  Keep only the columns we want
    #   cols = ['event_day', 'firm', 'action']
    # -------------------------------------------------------- 
    # <COMPLETE THIS PART>
    cols = ['event_day', 'firm', 'action']
    # -------------------------------------------------------- 
    #  Keep only values of 'action' that are either 'up' or 'down'
    # -------------------------------------------------------- 
    final_df = rec_df.loc[rec_df['action'].str.contains('up|down')]

    # -------------------------------------------------------- 
    #  Return the dataframe
    # -------------------------------------------------------- 
    return final_df[cols]

    


def mk_event_df(rec_df):
    """ This function takes a dataframe with the upgrades and downgrades
    for a given ticker and performs the following actions **in this order**:


    1. Create a column called 'score', which will take the following values:
        - 'score' = 1 iff `rec_df['action']` == 'up'
        - 'score' = -1 iff `rec_df['action']` == 'down'

    3. For each group defined by the values of the tuple (<event_day>, <firm>), 
       where '<event_day>' and '<firm>' represent the values taken by the
       elements in the columns 'event_day' and 'firm', sum the values of the 
       column 'score'.

    4. Create a column called 'event_type', which takes the following values:
        - 'event_type' = 'upgrade' if sum of 'score' above is positive
        - 'event_type' = 'downgrade' if sum of 'score' above is negative
        - 'event_type' is empty otherwise

    5. Keep only observations where the value of 'event_type' is either
       'upgrade' or 'downgrade'.

    6. Create a new column called 'event_id' containing the "ID" for each
       event (i.e. each unique combination of 'event_day' and 'firm').
       This event ID should start at 1
       
    7. Return the resulting dataframe, as specified in the Returns section
       below.

    Parameters
    ----------
    rec_df : dataframe
        Dataframe produced by the function `proc_rec_df` created above.
    

    Returns
    -------
    df 
        A Pandas dataframe with the following structure:

        - df.index : Some index uniquely identifying each row in the dataframe

        - df.columns : columns 'event_day', 'firm', and 'event_type' (in this
             order, created per instructions above.

        - df['event_id']: Integers representing the ID of this event, that is,
            uniquely identifying a unique combination of values (<event_day>,
            <firm>). The event ID should start at 1.

    Notes
    -----

    - The output of this function is a dataframe that looks like this (the
      contents of the df below are for illustration purposes only and will
      **not** represent the actual contents of the dataframe you create
      necessarily. In addition, the actual dataframe returned could be empty):


        |                | event_id | event_day  | firm           | event_type |
        | index          |          |            |                |            |
        | 0              | 1        | 2012-02-16 | Wunderlich     | downgrade  |
        | 1              | 2        | 2012-03-26 | Wunderlich     | upgrade    |
        | 2              | 3        | 2020-07-28 | Bernstein      | downgrade  |
        | 3              | 4        | 2020-08-14 | Morgan Stanley | upgrade    |
    
    The output of df.info() should look like this:

        Data columns (total 4 columns):
         #   Column      Non-Null Count  Dtype 
        ---  ------      --------------  ----- 
         0   event_id    42 non-null     int64 
         1   event_day   42 non-null     object
         2   firm        42 non-null     object
         3   event_type  42 non-null     object

    """
    # -------------------------------------------------------- 
    #   Create the score column
    # -------------------------------------------------------- 
    # <COMPLETE THIS PART>
    rec_df.loc[rec_df['action'].str.contains('up'), 'score'] = 1
    rec_df.loc[rec_df['action'].str.contains('down'), 'score'] = -1
    # -------------------------------------------------------- 
    #   Create group obj
    # -------------------------------------------------------- 
    # <COMPLETE THIS PART>
    hi_df = rec_df.groupby(['event_day', 'firm'])[['score']].sum()
    # -------------------------------------------------------- 
    #   Create the event_type column and keep only the rows for
    #   which event_types takes the values 'downgrade' or 'upgrade'
    # -------------------------------------------------------- 
    # <COMPLETE THIS PART>
    hi_df.loc[hi_df['score'] > 0, 'event_type'] = 'upgrade'
    hi_df.loc[hi_df['score'] < 0, 'event_type'] = 'downgrade'
    hi_df.loc[hi_df['score'] == 0, 'event_type'] = ''
    hi_df = hi_df.loc[hi_df['event_type'].str.contains('upgrade|downgrade')]
    # -------------------------------------------------------- 
    #   Create the event_id column 
    # -------------------------------------------------------- 
    # <COMPLETE THIS PART>
    hi_df.reset_index(inplace=True)
    hi_df = hi_df.rename_axis('event_id').reset_index()
    hi_df['event_id'] = hi_df['event_id'] + 1
    cols = ['event_id', 'event_day', 'firm', 'event_type']
    # -------------------------------------------------------- 
    #   Return the dataframe
    # -------------------------------------------------------- 
    # <COMPLETE THIS PART>
    return hi_df[cols]

    

# ---------------------------------------------------------------------------- 
#   Create a dataframe with event time
# ---------------------------------------------------------------------------- 
def mk_ret_dates_by_group(group):
    """ For a given group by event_id, preform the following operations (in
    this order):

    1. Create a column called "event_date", with the datetime representation
        of the dates in the 'event_day' column.

    1. Create another column called "ret_date" with the **datetime**
       representation of the relevant calendar date for the window surrounding
       the event. The calendar date will be the date in "event_date" plus number
       of days specified in the column "event_time". You may have to use the
       method 'pandas.to_timedelta'.

    Parameters
    ----------
    group : 
       a group defined by a value of event_id
    

    Returns
    -------
    dataframe
        A Pandas dataframe with the columns 
        ['event_id', 'firm', 'event_date', 'event_time', 'ret_date', 'event_type']


    Notes
    -----
 
    For instance, given a group like 
 
 
     | event_id | firm       | event_day  | event_type | event_time |
     |----------+------------+------------+------------+------------|
     | 1        | Wunderlich | 2012-02-16 | downgrade  | -2         |
     | 1        | Wunderlich | 2012-02-16 | downgrade  | -1         |
     | 1        | Wunderlich | 2012-02-16 | downgrade  | 0          |
     | 1        | Wunderlich | 2012-02-16 | downgrade  | 1          |
     | 1        | Wunderlich | 2012-02-16 | downgrade  | 2          |
 

    This function would produce the following data:
 
 
     | event_id | firm       | event_date | event_time | ret_date   | event_type |
     |----------+------------+------------+------------+------------+------------|
     | 1        | Wunderlich | 2012-02-16 | -2         | 2012-02-14 | downgrade  |
     | 1        | Wunderlich | 2012-02-16 | -1         | 2012-02-15 | downgrade  |
     | 1        | Wunderlich | 2012-02-16 | 0          | 2012-02-16 | downgrade  |
     | 1        | Wunderlich | 2012-02-16 | 1          | 2012-02-17 | downgrade  |
     | 1        | Wunderlich | 2012-02-16 | 2          | 2012-02-18 | downgrade  |

     which should be stored in a dataframe with the following characteristics:

     ----------------------------------------------
     Data columns (total 5 columns):
      #   Column      Non-Null Count  Dtype         
     ---  ------      --------------  -----         
      0   event_id    5 non-null      int64         
      1   firm        5 non-null      object        
      2   event_date  5 non-null      datetime64[ns]
      3   event_time  5 non-null      int64         
      4   ret_date    5 non-null      datetime64[ns]
      5   event_type  5 non-null      object
     ----------------------------------------------


    """
    # --------------------------------------------------------     
    # Leave this here
    # -------------------------------------------------------- 
    cols = ['event_id', 'firm', 'event_date', 'event_time', 'ret_date', 'event_type']

    # --------------------------------------------------------     
    # Create the event date col
    # -------------------------------------------------------- 
    # Create the event date col
    group.loc[:, 'event_date'] = pd.to_datetime(group.loc[:, 'event_day'])

    # Create the return date
    group.loc[:, 'ret_date'] = group.event_date + pd.to_timedelta(group.event_time, unit='day')

    # --------------------------------------------------------     
    # Leave this here
    # -------------------------------------------------------- 
    # keep only relevant columns
    group = group.loc[:, cols].copy()
    return group



# ---------------------------------------------------------------------------- 
#   You need to complete the docstring for this function
#   Do not modify the body of the function
# ---------------------------------------------------------------------------- 
def mk_ret_dates(event_df, window=2):
    """ This function takes a dataframe with the upgrades and downgrades
    for a given ticker (produced by the 'mk_event_df'
    and performs the following actions **in this order**:

    1. Create a DF which expands each row of event_df by (2 x window + 1) times
    2. Create a column called "event_time" with values from -`window` to `window` where the
    value of 0 is given to the "original 'event_id'" located at the middle of each expanded row. The earlier dates will
    begin from -2 and the latest date ends at 2.
    3. Create another column called "ret_date" with the **datetime**
      representation of the relevant calendar date. The calendar date will be
      the date in "event_date" plus the value from "event_time".

    Parameters
    ----------
    event_df : dataframe
        Dataframe produced by the function `mk_event_df` created above.

    Returns
    ------
    df
        A Pandas dataframe with the following structure:

        - df.index : Some index uniquely identifying each row in the dataframe, starting from 0.

        - df.columns : columns 'event_id', 'firm', 'event_day' and 'event_type' (in this
             order, created per instructions above.


    """
    # -------------------------------------------------------- 
    #   Expand the event_df 2 x window + 1 times
    # -------------------------------------------------------- 
    # This will expand each row of event_df from something like
    #
    # | event_id | firm       | event_day  | event_type |
    # |----------+------------+------------+------------|
    # | 1        | Wunderlich | 2012-02-16 | downgrade  |
    # 
    # To something like 
    # 
    # | event_id | firm       | event_day  | event_type |
    # |----------+------------+------------+------------|
    # | 1        | Wunderlich | 2012-02-16 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  |
    def mk_copies(row):
        return row.repeat(2*window+1)
    df = event_df.apply(mk_copies, axis=0)
    
    # -------------------------------------------------------- 
    #   Create an event_time column so that the DF now becomes
    # -------------------------------------------------------- 
    #
    # For each event_id:
    #
    # | event_id | firm       | event_day  | event_type | event_time |
    # |----------+------------+------------+------------+------------|
    # | 1        | Wunderlich | 2012-02-16 | downgrade  | -2         |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  | -1         |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  | 0          |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  | 1          |
    # | 1        | Wunderlich | 2012-02-16 | downgrade  | 2          |
    def mk_et(group):
        group['event_time'] = [i for i in range(-window, window+1)]
        return group
    groups = df.groupby('event_id')
    df = groups.apply(mk_et)

    # -------------------------------------------------------- 
    #   Create ret dates
    # -------------------------------------------------------- 
    # For each event_id, the df will look like this
    #
    # | event_id | firm       | event_date | event_time | ret_date   | event_type |
    # |----------+------------+------------+------------+------------+------------|
    # | 1        | Wunderlich | 2012-02-16 | -2         | 2012-02-14 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | -1         | 2012-02-15 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | 0          | 2012-02-16 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | 1          | 2012-02-17 | downgrade  |
    # | 1        | Wunderlich | 2012-02-16 | 2          | 2012-02-18 | downgrade  |
    groups = df.groupby('event_id')
    df = groups.apply(mk_ret_dates_by_group)
    df.index = range(len(df.index))

    return df.copy()


# ---------------------------------------------------------------------------- 
#   Function to create CARs. Do not modify this function
# ---------------------------------------------------------------------------- 
def mk_cars(tic, aret_df):
    """ For a given ticker, create compute the cumulative abnormal return (CAR) for each
    event ID and each event type (downgrade or upgrade).

    Parameters
    ----------
    tic : str
        Ticker in lower case characters and without spaces

    aret_df : dataframe
        A dataframe with abnormal returns (output of `mk_aret_df`.

    """
    # -------------------------------------------------------- 
    #   Get recommendations
    # -------------------------------------------------------- 
    rec_df = read_rec_csv(tic)
    rec_df = proc_rec_df(rec_df)

    # -------------------------------------------------------- 
    #   Create events
    # -------------------------------------------------------- 
    event_df = mk_event_df(rec_df)

    # -------------------------------------------------------- 
    #   Expand calendar dates dates for window surrounding the event
    # -------------------------------------------------------- 
    # Apply the function to each group
    event_df = mk_ret_dates(event_df)

    # -------------------------------------------------------- 
    #   Get abnormal returns for this ticker
    # -------------------------------------------------------- 
    # add returns
    # ---------- Leave this here -----
    tic = tic.lower().strip()
    aret_tic = aret_df.loc[:, [tic]].copy()
    # -------------------------------------------------------- 
    #   Create a column with calendar dates in the ARet datafrae
    # -------------------------------------------------------- 
    aret_tic.loc[:, 'ret_date'] = aret_tic.index.values
    # -------------------------------------------------------- 
    #   Rename the column with ARet from <tic> to 'aret'
    # -------------------------------------------------------- 
    aret_tic.rename(columns={tic: 'aret'}, inplace=True)

    # -------------------------------------------------------- 
    #   Add ARet to the event dataframe
    # -------------------------------------------------------- 
    event_df = event_df.join(aret_tic, how='inner', on='ret_date', rsuffix='_')
    del event_df['ret_date_']

    # -------------------------------------------------------- 
    #   Create a column with the ticker
    # -------------------------------------------------------- 
    event_df.loc[:, 'tic'] = tic

    # -------------------------------------------------------- 
    #   compute cumulative abnormal returns for each value of 
    #   - event_id
    #   - event_type
    #   - ticker
    # --------------------------------------------------------- 
    groups = event_df.groupby(['event_id', 'event_type', 'tic'])
    cars = groups['aret'].sum().reset_index()
    cars.rename(columns={'aret': 'car'}, inplace=True)

    # --------------------------------------------------------     
    #   Return the CAR dataframe
    # -------------------------------------------------------- 
    return cars




# ---------------------------------------------------------------------------- 
#   Main function
# ---------------------------------------------------------------------------- 
def main():
    """ This function executes all the functions in this module in the
    correct order.
    
    """
    
    # -------------------------------------------------------- 
    #   Get the tickers
    # -------------------------------------------------------- 
    tickers = get_tics(TICKERS)

    # -------------------------------------------------------- 
    #   Create a dataframe with adj closing prices for each tic
    # -------------------------------------------------------- 
    kargs = {
            'tickers': tickers,
            'srcdir': SRCDIR, 
            'src_cols': SRC_COLS,
            'src_col_dtypes': SRC_COL_DTYPES,
            'src_col_widths': SRC_COL_WIDTHS,
            }
    prc_df = mk_prc_df(**kargs)

    # -------------------------------------------------------- 
    #   Create a dataframe with Abnormal returns for each tic
    # -------------------------------------------------------- 
    aret_df = mk_aret_df(prc_df)

    # -------------------------------------------------------- 
    #   Compile CARs for each event
    #   The dataframe cars will have the following form:
    #
    #   Data columns (total 4 columns):
    #    #   Column      Non-Null Count  Dtype  
    #   ---  ------      --------------  -----  
    #    0   event_id    1080 non-null   int64  
    #    1   event_type  1080 non-null   object 
    #    2   tic         1080 non-null   object 
    #    3   car         1080 non-null   float64
    # -------------------------------------------------------- 
    cars = pd.concat([mk_cars(t, aret_df) for t in tickers], ignore_index=True)

    # -------------------------------------------------------- 
    #   Calculate the average CAR by event_type
    # -------------------------------------------------------- 
    cars_by_etype = cars.groupby(['event_type'])[['car']].mean()
    print(cars_by_etype)
    

if __name__ == "__main__":
    main()
