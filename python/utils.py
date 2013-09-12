'''
Copyright 2013 Mark Dredze. All rights reserved.
This software is released under the 2-clause BSD license.
Mark Dredze, mdredze@cs.jhu.edu
'''
import getopt, sys

def containsOption(option, options):
	for entry in options:
		if entry[0].replace('=', '') == option:
			return True
	return False
	
def usage(args, command_line, options):
	help_option = ('help', 'Print this help message.', False)
	
	if not containsOption('help', options):
		options_with_help.append(help_option)

		
	return_string = command_line % (args[0])
	return_string += '\nOptions:\n'
	for entry in options:
		arg = entry[0]
		help = entry[1]
		required = entry[2]
		default = entry[3]
		return_string += '\t--%s (%s)' % (arg, help)
		if required:
			return_string += ' (Required)'
		if default:
			return_string += ' (default=%s)' % default
		return_string += '\n'
	
	return return_string
	
def parseCommandLine(options_with_help, command_line=''):
	options = []
	required_options = []
	help_option = ('help', 'Print this help message.', False, None)
	
	if not containsOption('help', options):
		options_with_help.append(help_option)
		
	for entry in options_with_help:
		option = entry[0]
		help = entry[1]
		required = entry[2]
		default = entry[3]
		
		options.append(option)
		required_options.append((option, required, default))
		
		
	optlist, remainder = getopt.getopt(sys.argv[1:], '', options)
	
	parameter_hash = {}
	# Put the optlist into a hash.
	for parameter, value in optlist:
		if (parameter.startswith('--')):
			parameter = parameter[2:]
		if (parameter.endswith('=')):
			parameter = parameter[:-1]
		parameter_hash[parameter] = value
	
	
	missing_option = False
	for option, required, default in required_options:
		if required:
			option = option.replace('=', '')
			if option not in parameter_hash:
				missing_option = True
				print 'Missing required option: %s' % option
		# If an option is missing, use the default.
		elif (option not in parameter_hash) and default:
			parameter_hash[option] = default

	if missing_option or 'help' in parameter_hash:
		print usage(sys.argv, command_line, options_with_help)
		sys.exit()
		
	if missing_option:
		sys.exit()
		
		
	return parameter_hash, remainder