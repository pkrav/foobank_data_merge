# FooBank data processing script

This is a simple script which allows to merge data from Google bucket.

## Description and thought process

As the language of my choice is Python I have decided to implement this task with pandas library and try to keep this script as simple as possible.

First of all I have started with development of this script with getting list of content which is stored in Google bucket and creation of data structure with all necessary links.

After that there were four parts of data merging process:

1. Get list of loans items and merge it together.
2. Load web-visitors
3. Load customers
4. Merge all previous data parts

During the first step I have loaded all loans into list of DataFrames and concatenated it into single list. After that I have to change values in `webvisit_id` column from `"(123456,)"` to `123456` format in order to merge it correctly with visitors data.

I have not found any issues with visitors data, so I just loaded it through `pd.read_csv`. The third step was a bit more trickier. Customers data is stored in incorrectly formatted json file where each string is correct json. I have considered two options to create DataFrame from this data. One option is to read this file and add missing symbols (commas and brackets), so that this file became correct json and create from it DataFrame with `read_json`. The other option is to create list of DataFrames from each string and concatenate all these DataFrames. I have chosen the second option and decided that it will be better to choose list concatenation with `pd.concat` than append all temporary DataFrames to one as it has better performance according to pandas documentation.

The final step I have completed with two consequent outer merges (with `pd.merge`). I have done outer merge because I think that it is easier to cut out redundant data than merge it again from the begining.

## Installation and Usage

There are two ways to run this script. Directly using python and in Docker container.

### Run with Docker

In order to run script with Docker you should install it first on your operation system. Then you can build image:

```bash
docker build -t foobank --build-arg BUCKET="bynk-data-engineering-task" --build-arg GOOGLE_AUTH_CODE="<your_google_auth_code>" .
```

Run Docker image and save results to `./output`:

```bash
docker run -v ${PWD}/output:/output foobank
```

### Run directly

Before running script you have to install all required python packages:

```bash
pip3 install -r requirements.txt
```

After that you can just run:

```bash
python3 merge_data.py
```
