twitter_stream_downloader
=========================

A simple Python script to download tweets from the Twitter streaming API. Works with API version 1.1.

The script to run is python/streaming_downloader.py.

This requires you to have a consumer_key, consumer_secret, access_token, and
access_token_secret. To obtain these:
- go to dev.twitter.com
- login and create a new application
- Create an access token
- This will give you all four of the above. Remember, do not share these with anyone.

If you run the script with the --help flag it will show valid options.

The code creates files as year/month/timestamp.gz at least once every 24
hours. Changing this behavior isn't too hard but requires modifying the code.

The code requires tweepy. I am using version 1.9.
https://github.com/tweepy/tweepy

The script also supports the flag "pid_file". This will create a file with the PID of the running job. This is useful if you want to create a cron job that watches the script to make sure it is still running.

stream_type: There are three supported stream types. location, keyword and sample. I didn't put in the username stream type, but it should be easy to add.

If you use a keyword file, the format should be:
track=keyword1,keyword2,keyword3 ...

Location files are similar:
locations=value1,value2,value3,value4

These files are provided using the "stream_filename" argument.

