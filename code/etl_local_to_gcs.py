from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import json
from datetime import datetime
import re
from pandarallel import pandarallel
import numpy as np

@task(retries=3)
def fetch_business(business_path: str) -> pd.DataFrame:
    df_b = pd.read_json(business_path, lines=True)
    return df_b

@task(log_prints=True)
def transform_business(df_b: pd.DataFrame) -> pd.DataFrame:
    drop_columns = ['hours','review_count','attributes']
    df_b = df_b.drop(drop_columns, axis=1)

    df_food_bus = df_b[df_b['categories'].str.contains(
              'Restaurants|Food',
              case=False, na=False)]
    df_food_bus = df_food_bus.rename(columns={'stars':'avg_stars'})
    df_food_bus = df_food_bus.astype({"avg_stars": int})
    return df_food_bus

@task(retries=3)
def fetch_review(review_path: str, df_b_trans: pd.DataFrame, chunk_size: int) -> pd.DataFrame:
    reviews = pd.read_json(review_path, lines=True,
        dtype={'review_id':str,'user_id':str,
            'business_id':str,'stars':int,
            'date':str,'text':str,'useful':int,
            'funny':int,'cool':int},
        chunksize=chunk_size)
    chunk_list = []
    for chunk_review in reviews:
        # Drop columns that aren't needed
        chunk_review = chunk_review.drop(['funny','cool'], axis=1)
        # Renaming column name to avoid conflict with business overall star rating
        chunk_review = chunk_review.rename(columns={'stars': 'review_stars'})
        # Inner merge with edited business file so only reviews related to the business remain
        chunk_merged = pd.merge(df_b_trans, chunk_review, on='business_id', how='inner')
        chunk_list.append(chunk_merged)
    # After trimming down the review file, concatenate all relevant data back to one dataframe
    df_r = pd.concat(chunk_list, ignore_index=True, join='outer', axis=0)
    return df_r

@task()
def count_words_matched(text, word_list):
    cnt = 0
    for word in word_list:
        regexp = rf"\b{word}\b"
        if re.search(regexp, text):
            cnt = cnt + 1
    return cnt

@task(log_prints=True)
def transform_review(df_r: pd.DataFrame) -> pd.DataFrame:
    ## filter restaurants of US
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
    df_r_usa=df_r.loc[df_r['state'].isin(states)]
    df_r_usa.date = pd.to_datetime(df_r_usa.date)

    # label reviews as positive or negative
    df_r_usa['sentiment'] = ''
    # label reviews as positive or negative
    df_r_usa.loc[df_r_usa.review_stars >=4, 'sentiment'] = 'positive'
    df_r_usa.loc[df_r_usa.review_stars ==3, 'sentiment'] = 'neutral'
    df_r_usa.loc[df_r_usa.review_stars <3, 'sentiment'] = 'negative'

    ## converts text field to lowercase and saves in a new text_cleansed column
    ## Removes unnecessary punctuation or replaces with spaces
    df_r_usa['text_cleansed'] = ''
    df_r_usa.loc[:, 'text_cleansed'] = df_r_usa.text.str.lower() \
        .str.replace('(\\n)|[!.,?-]', ' ', regex=True) \
        .str.replace("[-|':$\\~\"#%&\(\)*+/:;<=>?@\[\]^_`{}]",'', regex=True)
    return df_r_usa

def fetch_veg_words(veg_path: Path) -> list:
    df_veg = pd.read_csv(veg_path)
    veg_words = df_veg['word'].tolist()
    return veg_words

# old
# def words_matched(text):
#     words = []
#     word_list = fetch_veg_words("data/veg_words.csv")
#     for word in word_list:
#         regexp = rf"\b{word}\b"
#         if re.search(regexp, text):
#             words.append(word)
#     return words

def words_matched(text):
    words = []
    word_list = fetch_veg_words("data/veg_words.csv")
    for word in word_list:
        regexp = rf"\b{word}\b"
        if re.search(regexp, text):
            words.append(word)
    return words

@task()
def split_df(df: pd.DataFrame, num_dfs: int) -> list:
    df_list = []
    factor = np.ceil(len(df) / num_dfs).astype(int)
    for i in range(num_dfs - 1):
        df_list.append(df.iloc[(i * factor): ((i + 1) * factor)])
    df_list.append(df.iloc[((num_dfs - 1) * factor):])
    return df_list

@task()
def write_local(df: pd.DataFrame, to_path: Path) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = to_path
    df.to_parquet(path, compression="gzip")
    return path

@task(retries=3)
def write_gcs(path: Path, to_path_gcs: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=to_path_gcs)
    return


@flow()
def etl_local_to_gcs() -> None:    
    """The main ETL function"""
    business_loc_path = "data/yelp_academic_dataset_business.json"
    review_loc_path = "data/yelp_academic_dataset_review.json"
    veg_words_loc_path = "data/veg_words.csv"
    chunk_size = 500000
    to_path_gcs = "yelp_project/data"
    
    df_b = fetch_business(business_loc_path)
    df_b_trans = transform_business(df_b)
    df_r = fetch_review(review_loc_path, df_b_trans, chunk_size)
    df_clean = transform_review(df_r)

    # Veg words matched
    def join_words(arr):
        return ' '.join(arr)
    
    pandarallel.initialize()
    words_matched_obj = df_clean['text_cleansed'].parallel_apply(words_matched)
    l_num_matches = list(map(len, words_matched_obj))
    df_clean.loc[:,'num_words_matched'] = l_num_matches
    l_words_matched_joined = list(map(join_words, words_matched_obj))
    df_clean.loc[:,'text_matched'] = l_words_matched_joined

    # old
    # df_clean['num_words_matched'] = df_clean['text_cleansed'].parallel_apply(count_words_matched)
    print(f"Num matched: {len(df_clean[df_clean['num_words_matched']>0])}")
    print(f"Num text_matched: {len(df_clean[df_clean['text_matched']!=''])}")
    
    # Split df to smaller dfs
    df_list = split_df(df_clean, 4)
    
    # write the files locally
    for file_num in range(4):
        file_name = f"reviews_{file_num}.parquet"
        from_path = write_local(df_list[file_num], f"data/reviews/{file_name}")
        write_gcs(from_path, f"{to_path_gcs}/reviews/{file_name}")
    write_gcs(veg_words_loc_path, f"{to_path_gcs}/veg_words.csv")

if __name__ == "__main__":
    etl_local_to_gcs()
