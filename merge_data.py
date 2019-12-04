import argparse
import json
import re
import requests
import pandas as pd
import time


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bucket',
        help='FooBank\'s Google bucket name',
        default='bynk-data-engineering-task'
    )
    parser.add_argument(
        '--auth', required=True,
        help='Google authorization code'
    )

    args = parser.parse_args()
    return args


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        res = func(*args, **kwargs)
        end = time.perf_counter()
        print("{} time: {: 0.4f} sec".format(func.__name__, end - start))
        return res
    return wrapper


def build_content_items(response):
    content = {}
    loans = []
    key_re = re.compile('data/(loan|customers|visits)')
    for item in response.json()["items"]:
        key_match = key_re.match(item["name"])
        if key_match:
            element = {
                "name": item["name"],
                "mediaLink": item["mediaLink"],
                "contentType": item["contentType"]
            }
            if key_match[1] in ('customers', 'visits'):
                content[key_match[1]] = element
            else:
                loans.append(element)

    content["loans"] = loans
    return content


def get_bucket_content(bucket, auth_code) -> dict:
    response = requests.get(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o",
        headers={
            "Authorize": f"Bearer {auth_code}"
        }
    )
    content = build_content_items(response)
    return content


def get_data_item(item):
    return pd.read_csv(
        item["mediaLink"],
        usecols=[
            "id", "user_id", "timestamp", "loan_amount",
            "loan_purpose", "outcome", "interest", "webvisit_id"
        ],
        dtype=object
    )


@timer
def merge_loans(content):
    loans = [get_data_item(item) for item in content]
    df_loans = pd.concat(
        loans, ignore_index=True, sort=False
    )

    re_id = re.compile(r'\((\d+),\)')
    df_loans[["webvisit_id"]] = pd.DataFrame(
        df_loans["webvisit_id"].apply(
            lambda x: x if pd.isnull(x) else str(int(re_id.findall(x)[0]))
        ),
        index=df_loans.index,
        dtype=object
    )
    df_loans["user_id"] = df_loans["user_id"].astype(int)
    return df_loans


@timer
def get_visits(item):
    df_visits = pd.read_csv(
        item["mediaLink"],
        usecols=["id", "timestamp", "referrer", "campaign_name"],
        dtype=object
    )

    return df_visits


@timer
def get_customers(item):
    df_customers = pd.DataFrame(dtype=object)
    response = requests.get(item["mediaLink"])

    df_customers = pd.concat(
        [
            pd.DataFrame.from_records([json.loads(line)])
            for line in response.text.split('\n')
        ],
        names=["id", "name", "ssn", "birthday", "gender", "city", "zip_code"],
        axis=0,
        sort=False,
        ignore_index=True
    )
    df_customers["id"] = df_customers["id"].astype(object)
    return df_customers


@timer
def merge_data(loans, visits, customers):
    loans_and_visits = pd.merge(
        loans,
        visits,
        left_on='webvisit_id',
        right_on='id',
        how='outer',
        suffixes=('_loan', '_visit')
    )
    loans_visits_customers = pd.merge(
        loans_and_visits,
        customers,
        left_on='user_id',
        right_on='id',
        how='outer',
        suffixes=('', '_customer')
    )
    return loans_visits_customers


@timer
def main():
    args = parse_arguments()

    content = get_bucket_content(args.bucket, args.auth)
    df_loans = merge_loans(content["loans"])
    df_visits = get_visits(content["visits"])
    df_customers = get_customers(content["customers"])

    result_dataset = merge_data(df_loans, df_visits, df_customers)
    result_dataset.to_csv("/output/loans_visits_customers.csv")


if __name__ == '__main__':
    main()
