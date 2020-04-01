'''
Copyright 2020 Mark Dredze. All rights reserved.
This software is released under the 2-clause BSD license.
Mark Dredze, mdredze@cs.jhu.edu
'''
from time import gmtime, strftime, localtime
import http
import gzip, logging, datetime
import time
import os
import argparse

import tweepy


class FileListener(tweepy.streaming.StreamListener):
	def __init__(self, path, restart_time):
		self.path = path
		self.current_file = None
		self.restart_time = restart_time
		self.file_start_time = time.time()
		self.file_start_date = datetime.datetime.now()
		
	def on_data(self, data):
		current_time = datetime.datetime.now()
		if self.current_file == None or time.time() - self.restart_time > self.file_start_time \
				or self.file_start_date.day != current_time.day:
			self.startFile()
			self.file_start_date = datetime.datetime.now()
		if data.startswith('{'):
			self.current_file.write(data)
			if not data.endswith('\n'):
				self.current_file.write('\n')

	def on_error(self, status):
		logging.error(status)

	def startFile(self):
		if self.current_file:
			self.current_file.close()
		
		local_time_obj = localtime()
		datetime = strftime("%Y_%m_%d_%H_%M_%S", local_time_obj)
		year = strftime("%Y", local_time_obj)
		month = strftime("%m", local_time_obj)
		
		full_path = os.path.join(self.path, year)
		full_path = os.path.join(full_path, month)
		try:
			os.makedirs(full_path)
			logging.info('Created %s' % full_path)
		except:
			pass
		filename = os.path.join(full_path, '%s.gz' % datetime)
		self.current_file = gzip.open(filename, 'w')
		self.file_start_time = time.time()
		logging.info('Starting new file: %s' % filename)


def load_stream_parameters(parameters_filename, stream_type):
	with open(parameters_filename, 'r') as input:
		content = '\n'.join(input.readlines())

	index = content.find('=')
	if index != -1:
		content = content[index+1:]
	return_value = content.split(',')
	
	if stream_type.lower() == 'location':
		for ii, entry in enumerate(return_value):
			return_value[ii] = float(entry)
	
	return return_value


def main():
	parser = argparse.ArgumentParser(description='Download streaming data from Twitter.')
	parser.add_argument('--consumer-key', required=True, help='the consumer key')
	parser.add_argument('--consumer-secret', required=True, help='the consumer key secret')
	parser.add_argument('--access-token', required=True, help='the access token')
	parser.add_argument('--access-token-secret', required=True, help='the access token secret')
	parser.add_argument('--stream-type', choices=['sample', 'location', 'keyword'], required=True,
						help='the type of stream to run')
	parser.add_argument('--output-directory', required=True, help='where to save the output files')
	parser.add_argument('--parameters-filename', required=False,
						help='file containing parameters for the stream (required for location and keyword.)')
	parser.add_argument('--pid-file', required=False, help='filename to store the process id')
	parser.add_argument('--log', required=False, help='log filename (default: write to console)')
	parser.add_argument('--log-level', required=False, default='INFO', choices=['CRITICAL', 'DEBUG', 'ERROR', 'FATAL', 'INFO', 'WARNING'], help='log filename (default: write to console)')

	args = parser.parse_args()

	# Setup the logger
	log_level = getattr(logging, args.loglevel.upper())
	log_format = '%(asctime)s %(levelname)s %(message)s'
	log_date_format = '%m/%d/%Y %I:%M:%S %p'

	if args.log:
		# Setup the logger to a file if one is provided
		logging.basicConfig(filename=args.log, level=log_level, format=log_format, datefmt=log_date_format)
	else:
		logging.basicConfig(level=log_level, format=log_format, datefmt=log_date_format)

	# Get the stream arguments
	if args.stream_type == 'location' or args.stream_type == 'keyword':
		if not args.parameters_filename:
			raise ValueError('--parameters-filename is requires when the stream is of type "location" or "keyword"')
		stream_parameters= load_stream_parameters(args.parameters_filename, args.stream_type)

	if args.pid_file:
		with open(args.pid_file, 'w') as writer:
			writer.write(str(os.getpid()))

	restart_time = 86400
	listener = FileListener(args.output_directory, restart_time)
	auth = tweepy.OAuthHandler(args.consumer_key, args.consumer_secret)
	auth.set_access_token(args.access_token, args.access_token_secret)


	try:
		while True:
			try:
				logging.info('Connecting to the stream of type {}'.format(args.stream_type))
				stream = tweepy.Stream(auth, listener)

				if args.stream_type == 'location':
					stream.filter(locations=stream_parameters)
				elif args.stream_type == 'keyword':
					stream.filter(track=stream_parameters)
				else:
					stream.sample()
			except http.client.IncompleteRead as e:
				logging.error('Exception: ' + str(e))
	except Exception as e:
		logging.error('Exception: ' + str(e))
	logging.info('Exiting.')


if __name__ == '__main__':
	main()