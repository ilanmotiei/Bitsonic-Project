
import slate
from itertools import groupby
import pandas as pd
import re
import functools


PDF_PATH = "Data/Amy Rigby Wrtr State 063020.pdf"
ARTIST_NAME = 'Amy Rigby'


def translate_pages(filepath=PDF_PATH):

    """
    :param filepath: The pdf file path.
    :return: A list that contains a text translation for every page in the pdf file.
    """

    with open(filepath, 'rb') as f:
        doc = slate.PDF(f)

    return doc


def text_to_tokens(text):

    """
    :param text: A text to process as a whole page.
    :return: A list of the relevant tokens extracted from it.
    """

    def process_token(t):
        t = t.lstrip(' ')
        return t

    tokens = text.split('\n')
    tokens = [process_token(t) for t in tokens if t != '']

    return tokens


def tokens_to_blocks(tokens):

    """
    :param tokens: A list of tokens defining some page.
    :return: A list of lists of tokens, in which each item describes a block of information.
    """

    NUM_HEADER_TOKENS = 29

    tokens = tokens[NUM_HEADER_TOKENS:]  # without the unuseful header tokens
    blocks = [list(g) for k,g in groupby(tokens, lambda x:x=='Composition Total:') if not k]

    return blocks


def process_page(df, page_text, curr_title, curr_source):

    """
    :param df: The pandas.df in which we'll store all our data.
    :param page_text: A text to process as a whole page.
    :param curr_title: The current title of lines.
    :param curr_source: The current source of lines.

    :return: The current title and the current source of lines.
    """

    tokens = text_to_tokens(page_text)
    blocks = tokens_to_blocks(tokens)

    for block in blocks:
        df, curr_title, curr_source = process_block(df, block, curr_title, curr_source)

    return df, curr_title, curr_source


def process_block(df, block, curr_title, curr_source):

    """
    :param df: The pandas.df in which we'll store all our data.
    :param block: The current block we're working on.
    :param curr_title: The current title of lines.
    :param curr_source: The current source of lines.
    :return: The modified:  df, curr_title, curr_source
    """

    offset = 0

    # WE ARE ITERATING OVER THE LINES IN THE BLOCK

    while offset <= len(block) - 9:
        df, curr_title, curr_source, offset = process_line(df, block, curr_title, curr_source, offset)

    return df, curr_title, curr_source


def process_line(df, block, curr_title, curr_source, offset):
    """

    :param df: The pandas.df that stores all our data.
    :param block: The current block we are working on.
    :param curr_title: The current title of the lines we're processing.
    :param curr_source: The current source of the lines we're processing.
    :param offset: The current offset we are at in the block.
    :return: The modified:    df, curr_title, curr_source, offset
    """

    def token_type(t):

        """
        :param t: A token.
        :return: Its type out of ['NAME', 'MONEY', 'INT', 'DATE', 'FLOAT']
        """

        if re.search('[a-zA-Z]', t):
            # token is a name
            return 'NAME'
        elif '/' in t:
            # token is a date
            return 'DATE'
        elif '$' in t:
            # token represents money
            return 'MONEY'
        elif '.' in t:
            # token is a float number
            return 'FLOAT'
        else:
            # token is an integer
            return 'INT'

    # ---------- CHECKING IF A NEW TITLE IS BEING DEFINED -----------

    if curr_title is None:
        # WE ARE STARTING TO PARSE - A NEW TITLE IS BEING DEFINED. BLOCKS FIRST ELEMENT IS THE NEW TITLE
        curr_title = block[0]
        offset += 1

    if token_type(block[offset]) == 'NAME' and token_type(block[offset+1]) == 'NAME':
        # A NEW TITLE IS DEFINED. BLOCKS FIRST ELEMENT IS THE NEW TITLE
        curr_title = block[0]
        offset += 1

    # ---------- CHECKING IF A NEW SOURCE IS BEING DEFINED ------------

    if curr_source is None:
        # WE ARE STARTING TO PARSE - A NEW SOURCE IS BEING DEFINED
        curr_source = block[offset]
        offset += 1

    if token_type(block[offset]) == 'NAME':
        # A NEW SOURCE IS DEFINED
        curr_source = block[offset]
        offset += 1

    line_types = [token_type(t) for t in block[offset: offset + 9]]
    line_format_1 = ["FLOAT", "MONEY", "FLOAT", "INT", "FLOAT", "DATE", "NAME", "INT", "NAME"]

    if functools.reduce(lambda x, y: x and y, map(lambda a, b: a == b, line_types, line_format_1), True):
        # 'PRODUCT' IS SHOWN IN CURRENT LINE
        line = block[offset: offset + 9]  # BLOCK[OFFSET: OFFSET + 9] IS THE CURRENT LINE TO BE PROCESSED
        line = pd.DataFrame({'title': [curr_title],
                             'artist': [ARTIST_NAME],
                             'source': [curr_source],

                             'reference': [line[7]],
                             'product': [line[6]],
                             'income_type': [line[8]],
                             'income_period': [line[5]],
                             'rate': [line[4]],
                             'quantity': [line[3]],
                             'amount_received': [line[2]],
                             'percent_payable': [line[0]],
                             'amount_payable': [line[1]]})

        offset += 9

    else:
        # line_types == ["FLOAT", "MONEY", "FLOAT", "INT", "FLOAT", "DATE", "INT", "NAME"
        # 'PRODUCT' IS MISSING AT THE CURRENT LINE

        line = block[offset: offset + 8]  # BLOCK[OFFSET: OFFSET + 8] IS THE CURRENT LINE TO BE PROCESSED
        line = pd.DataFrame({'title': [curr_title],
                             'artist': [ARTIST_NAME],
                             'source': [curr_source],

                             'reference': [line[6]],
                             'income_type': [line[7]],
                             'income_period': [line[5]],
                             'rate': [line[4]],
                             'quantity': [line[3]],
                             'amount_received': [line[2]],
                             'percent_payable': [line[0]],
                             'amount_payable': [line[1]]})

        offset += 8

    df = df.append(line, ignore_index=True)

    return df, curr_title, curr_source, offset


if __name__ == "__main__":

    pages = translate_pages()

    curr_title = None
    curr_source = None

    out_df = pd.DataFrame(columns=['title',
                                   'artist',
                                   'source',
                                   'reference',
                                   'product',
                                   'income_type',
                                   'income_period',
                                   'rate',
                                   'quantity',
                                   'amount_received',
                                   'percent_payable',
                                   'amount_payable'])

    for page in pages:

        out_df, curr_title, curr_source = process_page(out_df,
                                                       page,
                                                       curr_title,
                                                       curr_source)

    out_df.to_csv(path_or_buf='result_new.csv')
