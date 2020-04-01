# Copyright 2020 Mark Dredze. All rights reserved.
# This software is released under the 2-clause BSD license.
# Mark Dredze <mdredze@cs.jhu.edu>

import tweepy

import argparse
import datetime
import http
import logging
import os
import time
import gzip


class ParameterFileModifiedException(Exception):
	pass


class FileListener(tweepy.streaming.StreamListener):
	def __init__(self, path, restart_time, parameters_filename=None):
		self.path = path
		self.current_file = None
		self.restart_time = restart_time
		self.file_start_time = time.time()
		self.file_start_date = datetime.datetime.now()
		self.parameters_filename = parameters_filename
		self.parameters_filename_last_modified_time = None

		if self.parameters_filename:
			self.reset_parameter_file_mtime()

	def _check_parameters_file_modification(self):
		if self.parameters_filename:
			current_mtime = os.path.getmtime(self.parameters_filename)
			if current_mtime != self.parameters_filename_last_modified_time:
				raise ParameterFileModifiedException()

	def _ensure_file(self):
		# Should we start a new file?
		start_new_file = False
		if not self.current_file:
			# There is no existing file
			start_new_file = True
		elif self.current_file.closed:
			# The existing file is closed
			start_new_file = True
		elif time.time() - self.restart_time > self.file_start_time:
			# The amount of time that has passed for a restart is due.
			start_new_file = True
		elif self.file_start_date.day != datetime.datetime.now().day:
			# It is a new day
			start_new_file = True

		if start_new_file:
			self._start_new_file()

	def on_data(self, data):
		self._ensure_file()
		self._check_parameters_file_modification()

		if data.startswith('{'):
			self.current_file.write(data)
			if not data.endswith('\n'):
				self.current_file.write('\n')

	def on_error(self, status):
		logging.error(status)

	def _start_new_file(self):
		if self.current_file and not self.current_file.closed:
			self.current_file.close()
		
		local_time_obj = time.localtime()
		current_datetime = time.strftime("%Y_%m_%d_%H_%M_%S", local_time_obj)
		year = time.strftime("%Y", local_time_obj)
		month = time.strftime("%m", local_time_obj)
		
		full_path = os.path.join(self.path, year)
		full_path = os.path.join(full_path, month)
		try:
			os.makedirs(full_path)
			logging.info('Created %s' % full_path)
		except FileExistsError:
			pass

		filename = os.path.join(full_path, '%s.gz' % current_datetime)
		self.current_file = gzip.open(filename, 'wt')
		self.file_start_time = time.time()
		logging.info('Starting new file: %s' % filename)
		self.file_start_date = datetime.datetime.now()

	def close_file(self):
		if self.current_file and not self.current_file.closed:
			logging.info('Closing current file')
			self.current_file.close()

	def reset_parameter_file_mtime(self):
		self.parameters_filename_last_modified_time = os.path.getmtime(self.parameters_filename)


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
	parser.add_argument('--log', help='log filename (default: write to console)')
	parser.add_argument('--log-level', default='INFO', choices=['CRITICAL', 'DEBUG', 'ERROR', 'FATAL', 'INFO', 'WARNING'], help='log filename (default: write to console)')
	parser.add_argument('--check-for-new-parameters', action='store_true',
						help='checks the parameters file timestamp every minute for changes')

	args = parser.parse_args()

	# Setup the logger
	log_level = getattr(logging, args.log_level.upper())
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
		stream_parameters = load_stream_parameters(args.parameters_filename, args.stream_type)

	if args.pid_file:
		with open(args.pid_file, 'w') as writer:
			writer.write(str(os.getpid()))

	restart_time = 86400
	if args.check_for_new_parameters:
		listener = FileListener(args.output_directory, restart_time, parameters_filename=args.parameters_filename)
	else:
		listener = FileListener(args.output_directory, restart_time)

	auth = tweepy.OAuthHandler(args.consumer_key, args.consumer_secret)
	auth.set_access_token(args.access_token, args.access_token_secret)

	try:
		while True:
			try:
				logging.info('Connecting to the stream of type {}'.format(args.stream_type))
				stream = tweepy.Stream(auth = auth, listener = listener)

				if args.stream_type == 'location':
					stream.filter(locations=stream_parameters)
				elif args.stream_type == 'keyword':
					stream.filter(track=stream_parameters)
				else:
					stream.sample()
			except http.client.IncompleteRead as e:
				logging.exception('Got IncompleteRead exception')
				listener.close_file()
			except ParameterFileModifiedException as e:
				# The parameter file changed. Reaload the parameters and create a new stream.
				logging.info('Parameters file has changed; reloading')
				listener.close_file()
				stream_parameters = load_stream_parameters(args.parameters_filename, args.stream_type)
				listener.reset_parameter_file_mtime()

	except Exception as e:
		logging.exception('Got exception on main handler')

	listener.close_file()
	logging.info('Exiting.')


if __name__ == '__main__':
	main()
